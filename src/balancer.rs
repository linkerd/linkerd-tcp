use super::{ConfigError, Path, namerd};
use super::connection::Connection;
use super::connector::{ConnectorFactoryConfig, ConnectorFactory, Connector, EndpointCtx,
                       ConnectingSocket};
use super::resolver::DstAddr;
use futures::{Future, Sink, Poll, Async, StartSend, AsyncSink};
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
    pub client: ConnectorFactoryConfig,
}

impl BalancerConfig {
    pub fn mk_factory(&self, handle: &Handle) -> Result<BalancerFactory, ConfigError> {
        let cf = self.client.mk_connector_factory(handle)?;
        Ok(BalancerFactory {
               minimum_connections: self.minimum_connections,
               connector_factory: Rc::new(RefCell::new(cf)),
           })
    }
}

/// Creates a balancer for
#[derive(Clone)]
pub struct BalancerFactory {
    minimum_connections: usize,
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
            connector: connector,
            last_result: init,
            active: active,
            retired: OrderMap::default(),
        };
        Ok(Balancer(Rc::new(RefCell::new(b))))
    }
}

/// Provisions new outbound connections to a replica set.
///
/// A
///
/// When a `Balancer` is cloned, it's internal state is shared.
#[derive(Clone)]
pub struct Balancer(Rc<RefCell<InnerBalancer>>);

impl Balancer {
    pub fn connect(&mut self) -> Connect {
        Connect::new(self.0.clone())
    }

    /// Keeps track of pending and established connections.
    ///
    ///
    fn poll_connecting(&mut self) -> ConnectionPollSummary {
        self.0.borrow_mut().poll_connecting()
    }
}

#[derive(Debug, Default)]
struct ConnectionPollSummary {
    pending: usize,
    connected: usize,
    failed: usize,
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
        let summary = inner.poll_connecting();
        trace!("start_send: NotReady: {:?}", summary);
        inner.last_result = update;
        Ok(AsyncSink::Ready)
    }

    /// Never completes.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let summary = self.poll_connecting();
        trace!("poll_complete: NotReady: {:?}", summary);
        Ok(Async::NotReady)
    }
}

struct InnerBalancer {
    dst_name: Path,
    minimum_connections: usize,
    connector: Connector,
    last_result: namerd::Result<Vec<DstAddr>>,
    active: OrderMap<net::SocketAddr, Endpoint>,
    retired: OrderMap<net::SocketAddr, Endpoint>,
}

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
                        ep.connected.push_back(conn)
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

        summary
    }
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

pub struct Connect {
    state: Option<ConnectState>,
}
enum ConnectState {
    Init(Rc<RefCell<InnerBalancer>>),
}
impl Connect {
    fn new(bal: Rc<RefCell<InnerBalancer>>) -> Connect {
        Connect { state: Some(ConnectState::Init(bal)) }
    }
}

impl Future for Connect {
    type Item = Connection<EndpointCtx>;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // TODO:
        // - Pick two active endpoints at random (cheaply).
        // - Score the two endpoints.
        // - Pick a winner endponit.
        let state = self.state
            .take()
            .expect("connect must not be polled after completion");
        match state {
            ConnectState::Init(_) => unimplemented!(),
        }
    }
}
