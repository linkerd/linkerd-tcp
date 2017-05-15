use super::{ConfigError, Path, Socket, namerd};
use super::client::{ClientConfig, Client, Connector, EndpointCtx, Connect};
use super::connection::{Connection, ConnectionCtx};
use super::resolver::{DstAddr, Resolver, Resolve};
use futures::{Future, BoxFuture, Sink, Poll, Async, StartSend, AsyncSink};
use ordermap::OrderMap;

use std::{cmp, io, net};
use std::boxed::Box;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct BalancerConfig {
    pub minimum_connections: usize,
    pub client: ClientConfig,
}

impl BalancerConfig {
    pub fn mk_factory(self) -> Result<BalancerFactory, ConfigError> {
        let client = self.client.mk_client()?;
        Ok(BalancerFactory {
               minimum_connections: self.minimum_connections,
               client: Rc::new(client),
           })
    }
}

pub struct BalancerFactory {
    minimum_connections: usize,
    /// TODO connectors should be decided for each destination.
    client: Rc<Client>,
}

impl BalancerFactory {
    pub fn mk_factory(self, dst_name: Path, addrs: Vec<DstAddr>) -> Balancer {
        let connector = self.client.connector(dst_name);

        let active = {
            let mut active = OrderMap::with_capacity(addrs.len());
            for &DstAddr { addr, weight } in &addrs {
                let mut ep = Endpoint::default();
                ep.set_base_weight(weight);
                active.insert(addr, ep);
            }
            active
        };

        let b = InnerBalancer {
            dst_name: dst_name,
            minimum_connections: self.minimum_connections,
            connector: connector,
            active: active,
            retired: OrderMap::default(),
        };
        Balancer(Rc::new(RefCell::new(b)))
    }
}

#[derive(Clone)]
pub struct Balancer(Rc<RefCell<InnerBalancer>>);

struct InnerBalancer {
    dst_name: Path,
    minimum_connections: usize,
    connector: Connector,
    active: OrderMap<net::SocketAddr, Endpoint>,
    retired: OrderMap<net::SocketAddr, Endpoint>,
}

impl Balancer {
    pub fn connect(&mut self) -> Connect {
        // TODO:
        // - Pick two active endpoints at random (cheaply).
        // - Score the two endpoints.
        // - Pick a winner endponit.
        unimplemented!()
    }

    /// Keeps track of pending and established connections.
    ///
    ///
    fn poll_connecting(&mut self) -> ConnectionPollSummary {
        let mut summary = ConnectionPollSummary::default();

        let InnerBalancer {
            mut active,
            mut retired,
            connector,
            minimum_connections,
            ..
        } = *self.0.borrow_mut();

        for ep in active.values_mut() {
            for _ in 0..ep.connecting.len() {
                let mut fut = ep.connecting.pop_front().unwrap();
                match fut.poll() {
                    Ok(Async::NotReady) => ep.connecting.push_back(fut),
                    Ok(Async::Ready(sock)) => {
                        ep.record_connect_success();
                        ep.connected.push_back(sock)
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

        while !active.is_empty() && summary.connected + summary.pending < minimum_connections {
            for (addr, mut ep) in active.iter_mut() {
                let mut fut = connector.connect(ep.new_ctx());
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
                if summary.connected + summary.pending == minimum_connections {
                    break;
                }
            }
        }

        summary
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
        if let Ok(dsts) = update {
            let mut dsts = {
                let mut map = OrderMap::with_capacity(dsts.len());
                for &DstAddr { addr, weight } in &dsts {
                    map.insert(addr, weight);
                }
                map
            };

            let InnerBalancer {
                mut active,
                mut retired,
                ..
            } = *self.0.borrow_mut();
            let mut temp = VecDeque::with_capacity(cmp::max(active.len(), retired.len()));

            // Check retired endpoints.
            //
            // Endpoints are either salvaged backed into the active pool, maintained as
            // retired if still active, or dropped if inactive.
            {
                for (addr, ep) in retired.drain(..) {
                    if dsts.contains_key(&addr) {
                        active.insert(addr, ep);
                    } else if ep.active() > 0 {
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
                    } else if ep.active() > 0 {
                        let mut ep = ep;
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
            for (addr, weight) in dsts.drain(..) {
                let mut ep = active.entry(addr).or_insert_with(Endpoint::default);
                ep.set_base_weight(weight);
            }
        }

        // Ensure that
        let summary = self.poll_connecting();
        trace!("start_send: NotReady: {:?}", summary);
        Ok(AsyncSink::Ready)
    }

    /// Never completes.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let summary = self.poll_connecting();
        trace!("poll_complete: NotReady: {:?}", summary);
        Ok(Async::NotReady)
    }
}

struct Endpoint {
    base_weight: f32,
    ctx: EndpointCtx,
    connecting: VecDeque<BoxFuture<Socket, io::Error>>,
    connected: VecDeque<Socket>,
}

impl Endpoint {
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

impl Default for Endpoint {
    fn default() -> Endpoint {
        Endpoint {
            base_weight: 1.0,
            ctx: EndpointCtx::default(),
            connecting: VecDeque::new(),
            connected: VecDeque::new(),
        }
    }
}
