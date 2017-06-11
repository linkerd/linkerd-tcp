use super::DstAddr;
use super::dispatcher::{self, Dispatcher};
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
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use tacho::{self, Timing};
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

pub fn new(dst_name: Path,
           reactor: Handle,
           timer: Timer,
           connector: Connector,
           min_conns: usize,
           max_waiters: usize,
           waiters_rx: mpsc::UnboundedReceiver<DstConnectionRequest>,
           metrics: &tacho::Scope)
           -> Manager {
    let metrics_endpoints = metrics.clone().prefixed("endpoint");
    let metrics_connections = metrics.clone().prefixed("connection");

    let endpoints = Endpoints {
        available: OrderMap::default(),
        retired: OrderMap::default(),
        metrics: EndpointMetrics {
            attempts: metrics_connections.counter("attempts"),
            connects: metrics_connections.counter("connects"),
            failures: metrics_connections
                .clone()
                .labeled("cause", "other")
                .counter("failure"),
            timeouts: metrics_connections
                .clone()
                .labeled("cause", "timeout")
                .counter("failure"),
            connect_latency: metrics_connections.timer_us("latency_us"),
            connection_duration: metrics_connections.timer_ms("duration_ms"),
        },
    };
    let endpoints = Rc::new(RefCell::new(endpoints));
    let dst_name = Rc::new(dst_name);
    let dispatcher = dispatcher::new(dst_name,
                                     reactor.clone(),
                                     timer.clone(),
                                     connector,
                                     endpoints.clone(),
                                     max_waiters);
    Manager {
        dst_name,
        reactor,
        timer,
        dispatcher,
        endpoints,
        metrics: Metrics {
            available: metrics_endpoints.gauge("available"),
            failed: metrics_endpoints.gauge("failed"),
            retired: metrics_endpoints.gauge("retired"),
            load: metrics_endpoints.gauge("load"),
            connecting: metrics_connections.gauge("pending"),
            connected: metrics_connections.gauge("ready"),
            completing: metrics_connections.gauge("active"),
            waiters: metrics_connections.gauge("waiters"),
            poll_us: metrics.stat("poll_us"),
        },
    }
}

pub struct Endpoints {
    //minimum_connections: usize,
    /// Endpoints considered available for new connections.
    available: OrderMap<net::SocketAddr, Endpoint>,

    /// Endpoints that are still actvie but considered unavailable for new connections.
    retired: OrderMap<net::SocketAddr, Endpoint>,

    metrics: EndpointMetrics,
}

impl Endpoints {
    pub fn available(&self) -> &OrderMap<net::SocketAddr, Endpoint> {
        &self.available
    }
}

pub struct Manager {
    dst_name: Rc<Path>,

    reactor: Handle,
    timer: Timer,

    dispatcher: Dispatcher,
    endpoints: Rc<RefCell<Endpoints>>,
    metrics: Metrics,
}

struct Metrics {
    // // A cache of aggregated metadata about retired endpoints.
    // retired_meta: EndpointsMeta,
    available: tacho::Gauge,
    failed: tacho::Gauge,
    retired: tacho::Gauge,
    connecting: tacho::Gauge,
    connected: tacho::Gauge,
    completing: tacho::Gauge,
    load: tacho::Gauge,
    waiters: tacho::Gauge,
    poll_us: tacho::Stat,
}
impl Metrics {
    fn record(&self, meta: &EndpointsMeta) {
        self.connecting.set(meta.connecting);
        self.connected.set(meta.connected);
        self.completing.set(meta.completing);
        self.load.set(meta.load);
        self.waiters.set(meta.waiting);
        self.failed.set(meta.failed);
    }
}

// Caches aggregated metadata about a set of endpoints.
#[derive(Debug, Default)]
struct EndpointsMeta {
    waiting: usize,
    connecting: usize,
    connected: usize,
    completing: usize,
    failed: usize,
    load: usize,
}

#[derive(Clone)]
pub struct EndpointMetrics {
    pub attempts: tacho::Counter,
    pub connects: tacho::Counter,
    pub timeouts: tacho::Counter,
    pub failures: tacho::Counter,
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
        let handle = self.reactor.clone();
        let timer = self.timer.clone();
        for (addr, weight) in dsts.drain(..) {
            self.available
                .entry(addr)
                .or_insert_with(|| {
                                    Endpoint::new(name.clone(),
                                                  addr,
                                                  weight,
                                                  metrics.clone(),
                                                  handle.clone(),
                                                  timer.clone())
                                })
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
        // let t0 = Timing::start();

        // // Check all endpoints for updates..
        // self.manager.poll_endpoints();

        // // Try to resolve a new set of addresses. If there's an update, update the balancer's endpoints so that `available` contains only
        // if let Some(Ok(resolution)) = self.resolve() {
        //     self.manager.update_resolved(&resolution);
        // }

        // // Dispatch new select requests to available nodes.
        // self.manager.dispatch_new_selects();

        // // Initiate new connections.
        // self.manager.init_connecting();

        // self.manager.poll_us.add(t0.elapsed_us());
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
