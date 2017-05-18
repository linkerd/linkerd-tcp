use super::{DstConnection, DstAddr};
use super::endpoint::Endpoint;
use super::pool::{Pool, Waiter};
use super::super::Path;
use super::super::connector::Connector;
use super::super::resolver::{self, Resolve};
use futures::{Future, Stream, Sink, Poll, Async, StartSend, AsyncSink};
use futures::unsync::mpsc;
use ordermap::OrderMap;
use std::{cmp, net};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use tokio_core::reactor::Handle;

pub fn new(dst: Path,
           reactor: Handle,
           conn: Connector,
           min_conns: usize,
           on_dispatch: mpsc::UnboundedReceiver<()>,
           pool: Rc<Pool>)
           -> Manager {
    Manager {
        dst_name: dst,
        reactor: reactor,
        connector: conn,
        minimum_connections: min_conns,
        on_dispatch: OnDispatch::new(on_dispatch),
        pool: pool,
    }
}

pub struct Manager {
    dst_name: Path,
    reactor: Handle,
    connector: Connector,
    minimum_connections: usize,
    on_dispatch: OnDispatch,
    pool: Rc<Pool>,
}

impl Manager {
    pub fn manage(self, resolve: Resolve) -> Managing {
        Managing {
            manager: self,
            resolve: resolve,
        }
    }
}

impl Manager {
    fn poll_connecting(&mut self) -> ConnectionPollSummary {
        let mut active = self.pool.active.borrow_mut();
        let mut waiters = self.pool.waiters.borrow_mut();
        let mut summary = ConnectionPollSummary::default();

        for ep in active.values_mut() {
            for _ in 0..ep.connecting.len() {
                let mut fut = ep.connecting.pop_front().unwrap();
                match fut.poll() {
                    Ok(Async::NotReady) => ep.connecting.push_back(fut),
                    Ok(Async::Ready(conn)) => {
                        ep.ctx.connect_ok();
                        match send_to_waiter(conn, &mut waiters) {
                            Ok(_) => {
                                summary.dispatched += 1;
                            }
                            Err(conn) => {
                                ep.connected.push_back(conn);
                            }
                        }
                    }
                    Err(_) => {
                        ep.ctx.connect_fail();
                        summary.failed += 1
                    }
                }
            }
            summary.pending += ep.connecting.len();;
            summary.connected += ep.connected.len();
        }

        while !active.is_empty() && summary.connected + summary.pending < self.minimum_connections {
            for mut ep in active.values_mut() {
                let mut fut = self.connector.connect(ep.ctx.clone(), &self.reactor);
                // Poll the new connection immediately so that task notification is
                // established.
                match fut.poll() {
                    Ok(Async::NotReady) => {
                        ep.connecting.push_back(fut);
                        summary.pending += 1;
                    }
                    Ok(Async::Ready(sock)) => {
                        ep.ctx.connect_ok();
                        summary.connected += 1;
                        ep.connected.push_back(sock)
                    }
                    Err(_) => {
                        ep.ctx.connect_fail();
                        summary.failed += 1
                    }
                }
                if summary.connected + summary.pending == self.minimum_connections {
                    break;
                }
            }
        }

        // self.established_connections = summary.connected;
        // self.pending_connections = summary.pending;
        summary
    }

    pub fn update_resolved(&self, resolved: resolver::Result<Vec<DstAddr>>) {
        if let Ok(ref dsts) = resolved {
            let mut addr_weights = OrderMap::with_capacity(dsts.len());
            for &DstAddr { addr, weight } in dsts {
                addr_weights.insert(addr, weight);
            }
            self.update_endoints(addr_weights)
        }

        *self.pool.last_result.borrow_mut() = resolved;
    }

    fn update_endoints(&self, mut dsts: OrderMap<net::SocketAddr, f32>) {
        let mut active = self.pool.active.borrow_mut();
        let mut retired = self.pool.retired.borrow_mut();
        let mut temp = VecDeque::with_capacity(cmp::max(active.len(), retired.len()));

        // Check retired endpoints.
        //
        // Endpoints are either salvaged backed into the active pool, maintained as
        // retired if still active, or dropped if inactive.
        {
            for (addr, ep) in retired.drain(..) {
                if dsts.contains_key(&addr) {
                    active.insert(addr, ep);
                } else if ep.ctx.active() > 0 {
                    temp.push_back((addr, ep));
                } else {
                    drop(ep);
                }
            }
            for _ in 0..temp.len() {
                let (addr, ep) = temp.pop_front().unwrap();
                retired.insert(addr, ep);
            }
        }

        // Check active endpoints.
        //
        // Endpoints are either maintained in the active pool, moved into the retired poll if
        {
            for (addr, ep) in active.drain(..) {
                if dsts.contains_key(&addr) {
                    temp.push_back((addr, ep));
                } else if ep.ctx.active() > 0 {
                    let mut ep = ep;
                    // self.pending_connections -= ep.connecting.len();
                    // self.established_connections -= ep.connected.len();
                    ep.clear_connections();
                    retired.insert(addr, ep);
                } else {
                    drop(ep);
                }
            }
            for _ in 0..temp.len() {
                let (addr, ep) = temp.pop_front().unwrap();
                active.insert(addr, ep);
            }
        }

        // Add new endpoints or update the base weights of existing endpoints.
        let name = self.dst_name.clone();
        for (addr, weight) in dsts.drain(..) {
            let mut ep = active
                .entry(addr)
                .or_insert_with(|| Endpoint::new(name.clone(), addr, weight));
            ep.weight = weight;
        }
    }
}

fn send_to_waiter(conn: DstConnection,
                  waiters: &mut VecDeque<Waiter>)
                  -> Result<(), DstConnection> {
    if let Some(waiter) = waiters.pop_front() {
        return match waiter.send(conn) {
                   Err(conn) => send_to_waiter(conn, waiters),
                   Ok(()) => Ok(()),
               };
    }
    Err(conn)
}

pub struct Managing {
    manager: Manager,
    resolve: Resolve,
}

/// Balancers accept a stream of service discovery updates,
impl Future for Managing {
    type Item = ();
    type Error = ();

    /// Update the load balancer from service discovery.
    fn poll(&mut self) -> Poll<(), ()> {
        if let Async::Ready(update) = self.resolve.poll()? {
            match update {
                Some(resolved) => self.manager.update_resolved(resolved),
                None => panic!("resolve stream ended prematurely"),
            }
        }

        let summary = self.manager.poll_connecting();
        trace!("start_send: Ready: {:?}", summary);

        Ok(Async::NotReady)
    }
}

#[derive(Debug, Default)]
struct ConnectionPollSummary {
    pending: usize,
    connected: usize,
    dispatched: usize,
    failed: usize,
}

struct OnDispatch(mpsc::UnboundedReceiver<()>);
impl OnDispatch {
    pub fn new(recv: mpsc::UnboundedReceiver<()>) -> OnDispatch {
        OnDispatch(recv)
    }
}

impl Stream for OnDispatch {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Poll the undelrying at least once. If it is successful, poll until no longer successful.
        match self.0.poll()? {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(None) => Ok(Async::Ready(None)),
            Async::Ready(Some(_)) => {
                loop {
                    match self.0.poll()? {
                        Async::Ready(_) => {}
                        Async::NotReady => return Ok(Async::Ready(Some(()))),
                        Async::Ready(None) => return Ok(Async::Ready(None)),
                    }
                }
            }
        }
    }
}
