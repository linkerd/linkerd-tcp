use super::{ConfigError, Path, namerd};
use super::connection::Connection;
use super::connector::{DstConnection, ConnectorFactoryConfig, ConnectorFactory, Connector,
                       EndpointCtx, ConnectingSocket};
use super::resolver::DstAddr;
use futures::{Future, Sink, Poll, Async, StartSend, AsyncSink};
use futures::sync::oneshot;
use ordermap::OrderMap;
use std::{cmp, io, net};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use tokio_core::reactor::Handle;

#[derive(Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BalancerConfig {
    pub minimum_connections: usize,
    pub maximum_waiters: usize,
    pub client: ConnectorFactoryConfig,
}

impl BalancerConfig {
    pub fn mk_factory(&self, handle: &Handle) -> Result<BalancerFactory, ConfigError> {
        let cf = self.client.mk_connector_factory(handle)?;
        Ok(BalancerFactory {
               minimum_connections: self.minimum_connections,
               maximum_waiters: self.maximum_waiters,
               connector_factory: Rc::new(RefCell::new(cf)),
           })
    }
}

/// Creates a balancer for
#[derive(Clone)]
pub struct BalancerFactory {
    minimum_connections: usize,
    maximum_waiters: usize,
    connector_factory: Rc<RefCell<ConnectorFactory>>,
}
impl BalancerFactory {
    pub fn mk_balancer(&self,
                       dst_name: &Path,
                       init: namerd::Result<Vec<DstAddr>>)
                       -> Result<Balancer, ConfigError> {
        let connector = self.connector_factory.borrow().mk_connector(dst_name)?;

        let active = {
            if let Ok(ref addrs) = init {
                let mut active = OrderMap::with_capacity(addrs.len());
                for &DstAddr { addr, weight } in addrs {
                    active.insert(addr, Endpoint::new(dst_name.clone(), addr, weight));
                }
                active
            } else {
                OrderMap::new()
            }
        };

        let b = InnerBalancer {
            dst_name: dst_name.clone(),
            minimum_connections: self.minimum_connections,
            maximum_waiters: self.maximum_waiters,
            connector: connector,
            last_result: init,
            active: active,
            retired: OrderMap::default(),
            established_connections: 0,
            pending_connections: 0,
            waiters: VecDeque::with_capacity(self.maximum_waiters),
        };
        Ok(Balancer(Rc::new(RefCell::new(b))))
    }
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
        inner.add_waiter(sender);
        Connect(Some(ConnectState::Pending(receiver)))
    }

    /// Keeps track of pending and established connections.
    ///
    ///
    fn poll_connecting(&self) -> ConnectionPollSummary {
        self.0.borrow_mut().poll_connecting()
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
    type SinkItem = namerd::Result<Vec<DstAddr>>;
    type SinkError = ();

    /// Update the load balancer from service discovery.
    fn start_send(&mut self,
                  update: namerd::Result<Vec<DstAddr>>)
                  -> StartSend<namerd::Result<Vec<DstAddr>>, Self::SinkError> {
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
    dst_name: Path,
    minimum_connections: usize,
    maximum_waiters: usize,
    connector: Connector,
    last_result: namerd::Result<Vec<DstAddr>>,
    active: OrderMap<net::SocketAddr, Endpoint>,
    retired: OrderMap<net::SocketAddr, Endpoint>,
    pending_connections: usize,
    established_connections: usize,
    waiters: VecDeque<Waiter>,
}
type Waiter = oneshot::Sender<Connection<EndpointCtx>>;

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
                } else if ep.active() > 0 {
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
                } else if ep.active() > 0 {
                    let mut ep = ep;
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
            ep.set_base_weight(weight);
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
                        ep.record_connect_success();
                        match send_to_waiter(conn, &mut self.waiters) {
                            Ok(_) => {
                                summary.dispatched += 1;
                            }
                            Err(conn) => {
                                ep.connected.push_back(conn);
                            }
                        }
                    }
                    Err(e) => {
                        ep.record_connect_failure(e);
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
                let mut fut = self.connector.connect(ep.new_ctx());
                // Poll the new connection immediately so that task notification is
                // established.
                match fut.poll() {
                    Ok(Async::NotReady) => {
                        ep.connecting.push_back(fut);
                        summary.pending += 1;
                    }
                    Ok(Async::Ready(sock)) => {
                        ep.record_connect_success();
                        summary.connected += 1;
                        ep.connected.push_back(sock)
                    }
                    Err(e) => {
                        ep.record_connect_failure(e);
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

struct Endpoint {
    base_weight: f32,
    ctx: EndpointCtx,
    connecting: VecDeque<ConnectingSocket>,
    connected: VecDeque<Connection<EndpointCtx>>,
}

impl Endpoint {
    fn new(dst: Path, addr: net::SocketAddr, base_weight: f32) -> Endpoint {
        Endpoint {
            base_weight: base_weight,
            ctx: EndpointCtx::new(addr, dst),
            connecting: VecDeque::default(),
            connected: VecDeque::default(),
        }
    }

    fn new_ctx(&self) -> EndpointCtx {
        self.ctx.clone()
    }

    fn active(&self) -> usize {
        self.ctx.active()
    }

    fn clear_connections(&mut self) {
        self.connecting.clear();
        self.connected.clear();
    }

    fn set_base_weight(&mut self, weight: f32) {
        self.base_weight = weight;
    }

    fn record_connect_attempt(&mut self) {
        self.ctx.connect_init();
    }

    fn record_connect_success(&mut self) {
        self.ctx.connect_ok();
    }

    fn record_connect_failure(&mut self, _: io::Error) {
        self.ctx.connect_fail();
    }
}

pub struct Connect(Option<ConnectState>);
enum ConnectState {
    Pending(oneshot::Receiver<DstConnection>),
    Ready(DstConnection),
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
