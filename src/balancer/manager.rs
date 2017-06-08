use super::DstAddr;
use super::endpoint::Endpoint;
use super::selector::DstConnectionRequest;
use super::super::Path;
use super::super::connector::Connector;
use super::super::resolver::{self, Resolve};
use futures::{Future, Stream, Poll, Async};
use futures::unsync::mpsc;
use ordermap::OrderMap;
use rand::{self, Rng};
use std::{cmp, net};
use std::collections::VecDeque;
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

pub fn new(dst_name: Path,
           reactor: Handle,
           timer: Timer,
           connector: Connector,
           //min_conns: usize,
           selects: mpsc::UnboundedReceiver<DstConnectionRequest>,
           metrics: &tacho::Scope)
           -> Manager {
    let endpoints = metrics.clone().prefixed("endpoints");
    let connections = metrics.clone().prefixed("connections");
    Manager {
        dst_name,
        reactor,
        timer,
        connector,
        selects,
        waiter: None,
        available: OrderMap::default(),
        available_meta: EndpointsMeta::default(),
        retired: OrderMap::default(),
        available_gauge: endpoints.gauge("available"),
        retired_gauge: endpoints.gauge("retired"),
        connecting_gauge: connections.gauge("pending"),
        connected_gauge: connections.gauge("ready"),
        completing_gauge: connections.gauge("active"),
        waiters_gauge: connections.gauge("waiters"),
        endpoint_metrics: EndpointMetrics {
            connect_latency: connections.timer_us("latency_us"),
            connection_duration: connections.timer_ms("duration_ms"),
        },
    }
}

pub struct Manager {
    dst_name: Path,

    reactor: Handle,
    timer: Timer,

    connector: Connector,

    /// Requests from a `Selector` for a `DstConnection`.
    selects: mpsc::UnboundedReceiver<DstConnectionRequest>,
    waiter: Option<DstConnectionRequest>,

    //minimum_connections: usize,
    /// Endpoints considered available for new connections.
    available: OrderMap<net::SocketAddr, Endpoint>,

    /// Endpoints that are still actvie but considered unavailable for new connections.
    retired: OrderMap<net::SocketAddr, Endpoint>,

    // A cache of aggregated metadata about available endpoints.
    available_meta: EndpointsMeta,

    // // A cache of aggregated metadata about retired endpoints.
    // retired_meta: EndpointsMeta,
    available_gauge: tacho::Gauge,
    retired_gauge: tacho::Gauge,
    connecting_gauge: tacho::Gauge,
    connected_gauge: tacho::Gauge,
    completing_gauge: tacho::Gauge,
    waiters_gauge: tacho::Gauge,
    endpoint_metrics: EndpointMetrics,
}

// Caches aggregated metadata about a set of endpoints.
#[derive(Debug, Default)]
struct EndpointsMeta {
    waiting: usize,
    connecting: usize,
    connected: usize,
    completing: usize,
}

#[derive(Clone)]
pub struct EndpointMetrics {
    pub connect_latency: tacho::Timer,
    pub connection_duration: tacho::Timer,
}

impl Manager {
    pub fn manage(self, r: Resolve) -> Managing {
        Managing {
            manager: self,
            resolution: r,
        }
    }

    fn dispatch_new_selects(&mut self) {
        if let Some(waiter) = self.waiter.take() {
            if let Some(waiter) = self.dispatch_waiter(waiter) {
                debug!("cannot dispatch an endpoint in {}: no available endpoints",
                       self.dst_name);
                self.waiter = Some(waiter);
                return;
            }
        }

        loop {
            match self.selects.poll().expect("failed to select") {
                Async::NotReady |
                Async::Ready(None) => return,
                Async::Ready(Some(waiter)) => {
                    if let Some(waiter) = self.dispatch_waiter(waiter) {
                        debug!("cannot dispatch an endpoint in {}: no available endpoints",
                               self.dst_name);
                        self.waiter = Some(waiter);
                        return;
                    }
                }
            }
        }
    }

    fn dispatch_waiter(&mut self, waiter: DstConnectionRequest) -> Option<DstConnectionRequest> {
        if let Some(ep) = self.select_endpoint() {
            ep.track_waiting(waiter);
            return None;
        }
        Some(waiter)
    }

    /// Selects an endpoint using the power of two choices.
    ///
    /// We select 2 endpoints randomly, compare their weighted loads
    fn select_endpoint(&mut self) -> Option<&mut Endpoint> {
        match self.available.len() {
            0 => {
                trace!("no endpoints ready");
                None
            }
            1 => {
                // One endpoint, use it.
                self.available.get_index_mut(0).map(|(_, ep)| ep)
            }
            sz => {
                // Pick 2 candidate indices.
                let (i0, i1) = if sz == 2 {
                    // There are only two endpoints, so no need for an RNG.
                    (0, 1)
                } else {
                    // 3 or more endpoints: choose two distinct endpoints at random.
                    let mut rng = rand::thread_rng();
                    let i0 = rng.gen_range(0, sz);
                    let mut i1 = rng.gen_range(0, sz);
                    while i0 == i1 {
                        i1 = rng.gen_range(0, sz);
                    }
                    (i0, i1)
                };
                let addr = {
                    // Determine the index of the lesser-loaded endpoint
                    let (addr0, ep0) = self.available.get_index(i0).unwrap();
                    let (addr1, ep1) = self.available.get_index(i1).unwrap();
                    if ep0.weighted_load() <= ep1.weighted_load() {
                        trace!("dst: {} *{} (not {} *{})",
                               addr0,
                               ep0.weight(),
                               addr1,
                               ep1.weight());

                        *addr0
                    } else {
                        trace!("dst: {} *{} (not {} *{})",
                               addr1,
                               ep1.weight(),
                               addr0,
                               ep0.weight());
                        *addr1
                    }
                };
                self.available.get_mut(&addr)
            }
        }
    }

    fn poll_endpoints(&mut self) {
        let mut meta = EndpointsMeta::default();

        for mut ep in self.available.values_mut() {
            ep.poll_connecting();
            ep.poll_completing();
            ep.dispatch_waiting();
            meta.connecting += ep.connecting();;
            meta.connected += ep.connected();
            meta.completing += ep.completing();
            meta.waiting += ep.waiting()
        }
        for mut ep in self.retired.values_mut() {
            ep.poll_connecting();
            ep.poll_completing();
            ep.dispatch_waiting();
        }

        self.connecting_gauge.set(meta.connecting);
        self.connected_gauge.set(meta.connected);
        self.completing_gauge.set(meta.completing);
        self.waiters_gauge.set(meta.waiting);
        self.available_meta = meta;
    }

    /// Ensure that each endpoint has enough connections for all of its pending selecitons.
    pub fn init_connecting(&mut self) {
        for mut ep in self.available.values_mut().chain(self.retired.values_mut()) {
            // TODO should this be smarter somehow?
            let needed = ep.waiting();
            ep.init_connecting(needed, &self.connector, &self.reactor, &self.timer);
        }
    }

    // TODO: we need to do some sort of probation deal to manage endpoints that are
    // retired.
    pub fn update_resolved(&mut self, resolved: &[DstAddr]) {
        let mut temp = {
            let sz = cmp::max(self.available.len(), self.retired.len());
            VecDeque::with_capacity(sz)
        };
        let dsts = dsts_by_addr(resolved);
        self.check_retired(&dsts, &mut temp);
        self.check_available(&dsts, &mut temp);
        self.update_available_from_new(dsts);
        self.available_gauge.set(self.available.len());
        self.retired_gauge.set(self.retired.len());
    }

    /// Checks retired endpoints.
    ///
    /// Endpoints are either salvaged backed into the active pool, maintained as
    /// retired if still active, or dropped if inactive.
    fn check_retired(&mut self,
                     dsts: &OrderMap<net::SocketAddr, f32>,
                     temp: &mut VecDeque<Endpoint>) {
        for (addr, ep) in self.retired.drain(..) {
            if dsts.contains_key(&addr) {
                self.available.insert(addr, ep);
            } else if ep.is_idle() {
                drop(ep);
            } else {
                temp.push_back(ep);
            }
        }

        for _ in 0..temp.len() {
            let ep = temp.pop_front().unwrap();
            self.retired.insert(ep.peer_addr(), ep);
        }
    }

    /// Checks active endpoints.
    ///
    /// Endpoints are either maintained in the active pool, moved into the retired poll if
    fn check_available(&mut self,
                       dsts: &OrderMap<net::SocketAddr, f32>,
                       temp: &mut VecDeque<Endpoint>) {
        for (addr, ep) in self.available.drain(..) {
            if dsts.contains_key(&addr) {
                temp.push_back(ep);
            } else if ep.is_idle() {
                drop(ep);
            } else {
                // self.pending_connections -= ep.connecting.len();
                // self.established_connections -= ep.connected.len();
                self.retired.insert(addr, ep);
            }
        }

        for _ in 0..temp.len() {
            let ep = temp.pop_front().unwrap();
            self.available.insert(ep.peer_addr(), ep);
        }
    }

    fn update_available_from_new(&mut self, mut dsts: OrderMap<net::SocketAddr, f32>) {
        // Add new endpoints or update the base weights of existing endpoints.
        let name = self.dst_name.clone();
        let metrics = self.endpoint_metrics.clone();
        for (addr, weight) in dsts.drain(..) {
            self.available
                .entry(addr)
                .or_insert_with(|| Endpoint::new(name.clone(), addr, weight, metrics.clone()))
                .set_weight(weight);
        }
    }
}

fn dsts_by_addr(dsts: &[DstAddr]) -> OrderMap<net::SocketAddr, f32> {
    let mut by_addr = OrderMap::with_capacity(dsts.len());
    for &DstAddr { addr, weight } in dsts {
        by_addr.insert(addr, weight);
    }
    by_addr
}

pub struct Managing {
    manager: Manager,
    resolution: Resolve,
}

impl Managing {
    /// Obtains the most recent successful resolution.
    fn resolve(&mut self) -> Option<resolver::Result<Vec<DstAddr>>> {
        let mut resolution = None;
        loop {
            // Update the load balancer from service discovery.
            match self.resolution.poll().expect("unexpected resolver error") {
                Async::Ready(Some(Ok(result))) => {
                    resolution = Some(Ok(result));
                }
                Async::Ready(Some(Err(e))) => {
                    match resolution {
                        Some(Ok(_)) => {}
                        None | Some(Err(_)) => {
                            resolution = Some(Err(e));
                        }
                    }
                }
                Async::Ready(None) => {
                    error!("resolution completed unexpectedly");
                    return resolution;
                }
                Async::NotReady => {
                    return resolution;
                }
            }
        }
    }
}

/// Drives all work related to managing a load balancer.
///
/// Never completes.
///
/// Each time this is polled, the following occurs:
///
/// 1. All endpoints are checked for newly established or lost.
/// 2. The resolver is checked
impl Future for Managing {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        // TODO track a stat for execution time.

        // Check all endpoints for updates..
        self.manager.poll_endpoints();

        // Try to resolve a new set of addresses. If there's an update, update the balancer's endpoints so that `available` contains only
        if let Some(Ok(resolution)) = self.resolve() {
            self.manager.update_resolved(&resolution);
        }

        // Dispatch new select requests to available nodes.
        self.manager.dispatch_new_selects();

        // Initiate new connections.
        self.manager.init_connecting();

        Ok(Async::NotReady)
    }
}

///
#[derive(Debug, Default)]
struct ConnectionPollSummary {
    pending: usize,
    connected: usize,
    dispatched: usize,
    completed: usize,
    failed: usize,
}
