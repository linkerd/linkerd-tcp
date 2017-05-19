use super::{DstConnection, DstAddr};
//use super::endpoint::Endpoint;
//use super::pool::{Pool, Waiter};
use super::super::Path;
use super::super::connector::{Connector, Connecting};
use super::super::resolver::{self, Resolve};
use futures::{Future, Stream, Sink, Poll, Async, StartSend, AsyncSink};
use futures::unsync::{mpsc, oneshot};
use ordermap::OrderMap;
use std::{cmp, net};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use tokio_core::reactor::Handle;

pub type Waiter = oneshot::Sender<DstConnection>;

pub fn new(dst: Path,
           reactor: Handle,
           conn: Connector,
           min_conns: usize,
           dispatch: mpsc::UnboundedReceiver<Waiter>)
           -> Manager {
    let (tx, rx) = mpsc::unbounded();
    Manager {
        dst_name: dst,
        reactor: reactor,
        connector: conn,
        minimum_connections: min_conns,
        active: OrderMap::default(),
        retired: OrderMap::default(),
        dispatch: dispatch,
        completions_rx: rx,
        completions_tx: tx,
    }
}

pub struct ConnectionSummary {
    local_addr: net::SocketAddr,
    peer_addr: net::SocketAddr,
    read_bytes: usize,
    summary_bytes: usize,
}

pub struct Manager {
    dst_name: Path,

    reactor: Handle,

    connector: Connector,

    minimum_connections: usize,

    active: OrderMap<net::SocketAddr, Endpoint>,

    retired: OrderMap<net::SocketAddr, Endpoint>,

    dispatch: mpsc::UnboundedReceiver<Waiter>,

    completions_rx: mpsc::UnboundedReceiver<ConnectionSummary>,
    completions_tx: mpsc::UnboundedSender<ConnectionSummary>,
}

impl Manager {
    pub fn manage(self, resolve: Resolve) -> Managing {
        Managing {
            manager: self,
            resolve: resolve,
        }
    }
    fn dispatch(&mut self) {
        // If there are no endpoints to select from, we can't do anything.
        if self.active.is_empty() {
            // XXX we could fail these waiters here.  I'd prefer to rely on a timeout in
            // the dispatching task.
            return;
        }
        loop {
            match self.dispatch.poll() {
                Err(_) |
                Ok(Async::NotReady) |
                Ok(Async::Ready(None)) => return,
                Ok(Async::Ready(Some(waiter))) => {
                    let mut ep = self.select_endpoint();
                    ep.dispatch(waiter);
                }
            }
        }
    }

    fn select_endpoint(&mut self) -> &mut Endpoint {
        unimplemented!()
    }

    fn poll_connecting(&mut self) -> ConnectionPollSummary {
        let mut summary = ConnectionPollSummary::default();

        for ep in self.active.values_mut() {
            for _ in 0..ep.connecting.len() {
                let mut fut = ep.connecting.pop_front().unwrap();
                match fut.poll() {
                    Ok(Async::NotReady) => ep.connecting.push_back(fut),
                    Ok(Async::Ready(conn)) => {
                        // XXX ep.ctx.connect_ok();
                        match ep.send_to_waiter(conn) {
                            Ok(_) => {
                                summary.dispatched += 1;
                            }
                            Err(conn) => {
                                ep.connected.push_back(conn);
                            }
                        }
                    }
                    Err(_) => {
                        // XX ep.ctx.connect_fail();
                        summary.failed += 1
                    }
                }
            }
            summary.pending += ep.connecting.len();;
            summary.connected += ep.connected.len();
        }

        while !self.active.is_empty() &&
              summary.connected + summary.pending < self.minimum_connections {
            for mut ep in self.active.values_mut() {
                let mut fut = self.connector.connect(ep.ctx.clone(), &self.reactor);
                // Poll the new connection immediately so that task notification is
                // established.
                match fut.poll() {
                    Ok(Async::NotReady) => {
                        // XXX ep.connecting.push_back(fut);
                        summary.pending += 1;
                    }
                    Ok(Async::Ready(sock)) => {
                        // XXX ep.ctx.connect_ok();
                        summary.connected += 1;
                        ep.connected.push_back(sock)
                    }
                    Err(_) => {
                        // XXX ep.ctx.connect_fail();
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
        if let Ok(ref resolved) = resolved {
            let mut dsts = OrderMap::with_capacity(resolved.len());
            for &DstAddr { addr, weight } in resolved {
                dsts.insert(addr, weight);
            }

            let mut temp = VecDeque::with_capacity(cmp::max(self.active.len(), self.retired.len()));

            // Check retired endpoints.
            //
            // Endpoints are either salvaged backed into the active pool, maintained as
            // retired if still active, or dropped if inactive.
            {
                for (addr, ep) in self.retired.drain(..) {
                    if dsts.contains_key(&addr) {
                        self.active.insert(addr, ep);
                    } else if ep.ctx.active() > 0 {
                        temp.push_back((addr, ep));
                    } else {
                        drop(ep);
                    }
                }
                for _ in 0..temp.len() {
                    let (addr, ep) = temp.pop_front().unwrap();
                    self.retired.insert(addr, ep);
                }
            }

            // Check active endpoints.
            //
            // Endpoints are either maintained in the active pool, moved into the retired poll if
            {
                for (addr, ep) in self.active.drain(..) {
                    if dsts.contains_key(&addr) {
                        temp.push_back((addr, ep));
                    } else if ep.ctx.active() > 0 {
                        let mut ep = ep;
                        // self.pending_connections -= ep.connecting.len();
                        // self.established_connections -= ep.connected.len();
                        self.retired.insert(addr, ep);
                    } else {
                        drop(ep);
                    }
                }
                for _ in 0..temp.len() {
                    let (addr, ep) = temp.pop_front().unwrap();
                    self.active.insert(addr, ep);
                }
            }

            // Add new endpoints or update the base weights of existing endpoints.
            let name = self.dst_name.clone();
            for (addr, weight) in dsts.drain(..) {
                let mut ep = self.active
                    .entry(addr)
                    .or_insert_with(|| Endpoint::new(name.clone(), addr, weight));
                ep.weight = weight;
            }
        }
    }
}

struct Endpoint {
    weight: f32,
    load: f32,
    connecting: VecDeque<Connecting>,
    connected: VecDeque<DstConnection>,
    waiters: VecDeque<Waiter>,
}
impl Endpoint {
    fn new(dst: Path, addr: net::SocketAddr, weight: f32) -> Endpoint {
        Endpoint {
            weight: weight,
            load: ::std::f32::MAX,
            //ctx: EndpointCtx::new(addr, dst),
            connecting: VecDeque::default(),
            connected: VecDeque::default(),
            waiters: VecDeque::default(),
        }
    }

    fn send_to_waiter(&mut self, conn: DstConnection) -> Result<(), DstConnection> {
        if let Some(waiter) = self.waiters.pop_front() {
            return match waiter.send(conn) {
                       Err(conn) => self.send_to_waiter(conn),
                       Ok(()) => Ok(()),
                   };
        }
        Err(conn)
    }

    fn dispatch(&mut self, waiter: Waiter) {
        match self.connected.pop_front() {
            None => self.waiters.push_back(waiter),
            Some(conn) => {
                if let Err(conn) = waiter.send(conn) {
                    // waiter no longer waiting. save the connection for later.
                    self.connected.push_front(conn);
                }
            }
        }
    }
}

pub struct Managing {
    manager: Manager,
    resolve: Resolve,
}

impl Managing {}

/// Balancers accept a stream of service discovery updates,
impl Future for Managing {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        // First, check new connection requests:
        self.manager.dispatch();

        // Update the load balancer from service discovery.
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
