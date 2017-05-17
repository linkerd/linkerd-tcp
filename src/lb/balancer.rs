use super::endpoint::Endpoint;
use super::super::{DstConnection, DstAddr, Path, resolver};
use super::super::connector::Connector;
use futures::{Future, Sink, Poll, Async, StartSend, AsyncSink};
use futures::sync::oneshot;
use ordermap::OrderMap;
use std::{cmp, io, net};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use tokio_core::reactor::Handle;

pub fn new(reactor: Handle,
           dst: Path,
           min_conns: usize,
           max_waiters: usize,
           conn: Connector,
           last_result: resolver::Result<Vec<DstAddr>>)
           -> Balancer {
    let active = {
        if let Ok(ref addrs) = last_result {
            let mut active = OrderMap::with_capacity(addrs.len());
            for &DstAddr { addr, weight } in addrs {
                active.insert(addr, Endpoint::new(dst.clone(), addr, weight));
            }
            active
        } else {
            OrderMap::new()
        }
    };

    let b = InnerBalancer {
        reactor: reactor,
        dst_name: dst,
        minimum_connections: min_conns,
        maximum_waiters: max_waiters,
        connector: conn,
        last_result: last_result,
        active: active,
        retired: OrderMap::default(),
        established_connections: 0,
        pending_connections: 0,
        waiters: VecDeque::with_capacity(max_waiters),
    };
    Balancer(Rc::new(RefCell::new(b)))
}

/// Provisions new outbound connections to a replica set.
///
/// A `Balancer` is a `Sink` for destination resolutions. As new states are sent to the sink, the
///
/// When a `Balancer` is cloned, it's internal state is shared.
#[derive(Clone)]
pub struct Balancer(Rc<RefCell<InnerBalancer>>);

impl Balancer {
    /// Obtain an established connection immediately or wait until one becomes available.
    pub fn connect(&self) -> Connect {
        let mut inner = self.0.borrow_mut();
        if let Some(conn) = inner.take_connection() {
            return Connect(Some(ConnectState::Ready(conn)));
        }

        let (sender, receiver) = oneshot::channel();
        if let Err(sender) = inner.add_waiter(sender) {
            drop(sender);
            drop(receiver);
            Connect(Some(ConnectState::Failed(io::ErrorfKind::ConnectionReset.into())))
        } else {
            Connect(Some(ConnectState::Pending(receiver)))
        }
    }

    pub fn pending_connections(&self) -> usize {
        self.0.borrow().pending_connections
    }

    pub fn established_connections(&self) -> usize {
        self.0.borrow().established_connections
    }

    pub fn waiters(&self) -> usize {
        self.0.borrow().waiters.len()
    }
}

/// Balancers accept a stream of service discovery updates,
impl Sink for Balancer {
    type SinkItem = resolver::Result<Vec<DstAddr>>;
    type SinkError = ();

    /// Update the load balancer from service discovery.
    fn start_send(&mut self,
                  update: resolver::Result<Vec<DstAddr>>)
                  -> StartSend<resolver::Result<Vec<DstAddr>>, Self::SinkError> {
        let mut inner = self.0.borrow_mut();
        if let Ok(ref dsts) = update {
            let addr_weights = {
                let mut map = OrderMap::with_capacity(dsts.len());
                for &DstAddr { addr, weight } in dsts {
                    map.insert(addr, weight);
                }
                map
            };
            inner.update_endoints(addr_weights)
        }
        inner.last_result = update;
        let summary = inner.poll_connecting();
        trace!("start_send: Ready: {:?}", summary);
        Ok(AsyncSink::Ready)
    }

    /// Never completes.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let summary = self.0.borrow_mut().poll_connecting();
        trace!("poll_complete: NotReady: {:?}", summary);
        Ok(Async::NotReady)
    }
}

struct InnerBalancer {
    reactor: Handle,
    dst_name: Path,
    minimum_connections: usize,
    maximum_waiters: usize,
    connector: Connector,
    last_result: resolver::Result<Vec<DstAddr>>,
    active: OrderMap<net::SocketAddr, Endpoint>,
    retired: OrderMap<net::SocketAddr, Endpoint>,
    pending_connections: usize,
    established_connections: usize,
    waiters: VecDeque<Waiter>,
}
type Waiter = oneshot::Sender<DstConnection>;

impl InnerBalancer {
    fn update_endoints(&mut self, mut dsts: OrderMap<net::SocketAddr, f32>) {
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
                    self.pending_connections -= ep.connecting.len();
                    self.established_connections -= ep.connected.len();
                    ep.clear_connections();
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
            ep.base_weight = weight;
        }
    }

    fn poll_connecting(&mut self) -> ConnectionPollSummary {
        let mut summary = ConnectionPollSummary::default();

        for ep in self.active.values_mut() {
            for _ in 0..ep.connecting.len() {
                let mut fut = ep.connecting.pop_front().unwrap();
                match fut.poll() {
                    Ok(Async::NotReady) => ep.connecting.push_back(fut),
                    Ok(Async::Ready(conn)) => {
                        ep.ctx.connect_ok();
                        match send_to_waiter(conn, &mut self.waiters) {
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

        while !self.active.is_empty() &&
              summary.connected + summary.pending < self.minimum_connections {
            for mut ep in self.active.values_mut() {
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

        self.established_connections = summary.connected;
        self.pending_connections = summary.pending;
        summary
    }

    fn add_waiter(&mut self, w: Waiter) -> Result<(), Waiter> {
        if self.waiters.len() == self.maximum_waiters {
            return Err(w);
        }
        self.waiters.push_back(w);
        Ok(())
    }

    // XXX This isn't real load balancing.
    // 1. Select 2 endpoints at random.
    // 2. Score both endpoints.
    // 3. Take winner.
    fn take_connection(&mut self) -> Option<DstConnection> {
        for ep in self.active.values_mut() {
            if let Some(conn) = ep.connected.pop_front() {
                self.established_connections -= 1;
                return Some(conn);
            }
        }
        None
    }
}

#[derive(Debug, Default)]
struct ConnectionPollSummary {
    pending: usize,
    connected: usize,
    dispatched: usize,
    failed: usize,
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

pub struct Connect(Option<ConnectState>);
enum ConnectState {
    Pending(oneshot::Receiver<DstConnection>),
    Ready(DstConnection),
    Failed(io::Error),
}

impl Future for Connect {
    type Item = DstConnection;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // TODO:
        // - Pick two active endpoints at random (cheaply).
        // - Score the two endpoints.
        // - Pick a winner endponit.
        let state = self.0
            .take()
            .expect("connect must not be polled after completion");
        match state {
            ConnectState::Failed(e) => Err(e),
            ConnectState::Ready(conn) => Ok(conn.into()),
            ConnectState::Pending(mut recv) => {
                match recv.poll() {
                    Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
                    Ok(Async::Ready(conn)) => Ok(conn.into()),
                    Ok(Async::NotReady) => {
                        self.0 = Some(ConnectState::Pending(recv));
                        Ok(Async::NotReady)
                    }
                }
            }
        }
    }
}
