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
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

pub fn new(dst: Path,
           reactor: Handle,
           timer: Timer,
           conn: Connector,
           //min_conns: usize,
           selects: mpsc::UnboundedReceiver<DstConnectionRequest>)
           -> Manager {
    Manager {
        dst_name: dst,
        reactor: reactor,
        timer: timer,
        connector: conn,
        //minimum_connections: min_conns,
        available: OrderMap::default(),
        available_meta: EndpointsMeta::default(),
        retired: OrderMap::default(),
        selects: selects,
    }
}

pub struct Manager {
    dst_name: Path,

    reactor: Handle,
    timer: Timer,

    connector: Connector,

    /// Requests from a `Selector` for a `DstConnection`.
    selects: mpsc::UnboundedReceiver<DstConnectionRequest>,

    //minimum_connections: usize,
    /// Endpoints considered available for new connections.
    available: OrderMap<net::SocketAddr, Endpoint>,

    /// Endpointsthat are still actvie but considered unavailable for new connections.
    retired: OrderMap<net::SocketAddr, Endpoint>,

    // A cache of aggregated metadata about available endpoints.
    available_meta: EndpointsMeta,

    // // A cache of aggregated metadata about retired endpoints.
    // retired_meta: EndpointsMeta,
}

// Caches aggregated metadata about a set of endpoints.
#[derive(Debug, Default)]
struct EndpointsMeta {
    connecting: usize,
    connected: usize,
    completing: usize,
}

impl Manager {
    pub fn manage(self, r: Resolve) -> Managing {
        Managing {
            manager: self,
            resolution: r,
        }
    }

    fn dispatch_new_selects(&mut self) {
        // If there are no endpoints, we can't do anything.
        if self.available.is_empty() {
            info!("cannot dispatch an endpoint in {}: no available endpoints",
                  self.dst_name);
            // XXX we could fail these waiters here.  I'd prefer to rely on a timeout in
            // the dispatching task.
            return;
        }
        loop {
            match self.selects
                      .poll()
                      .expect("failed to read from unbounded queue") {
                Async::NotReady |
                Async::Ready(None) => return,
                Async::Ready(Some(get_conn)) => {
                    if let Some(ep) = self.select_endpoint() {
                        ep.track_waiting(get_conn);
                    } else {
                        //
                    }
                }
            }
        }
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
        }
        for mut ep in self.retired.values_mut() {
            ep.poll_connecting();
            ep.poll_completing();
            ep.dispatch_waiting();
        }

        self.available_meta = meta;
    }

    pub fn init_connecting(&mut self) {
        // Ensure that each endpoint has enough connections for all of its pending selecitons.
        for mut ep in self.available.values_mut().chain(self.retired.values_mut()) {
            let needed = ep.waiting() - (ep.connecting() + ep.connected());
            ep.init_connecting(needed, &self.connector, &self.reactor, &self.timer);
        }
    }

    // TODO: we need to do some sort of probation deal to manage endpoints that are
    // retired.
    pub fn update_resolved(&mut self, resolved: Vec<DstAddr>) {
        let mut dsts = OrderMap::with_capacity(resolved.len());
        for &DstAddr { addr, weight } in &resolved {
            dsts.insert(addr, weight);
        }

        let mut temp = {
            let sz = cmp::max(self.available.len(), self.retired.len());
            VecDeque::with_capacity(sz)
        };

        // Check retired endpoints.
        //
        // Endpoints are either salvaged backed into the active pool, maintained as
        // retired if still active, or dropped if inactive.
        {
            for (addr, ep) in self.retired.drain(..) {
                if dsts.contains_key(&addr) {
                    self.available.insert(addr, ep);
                } else if ep.is_idle() {
                    drop(ep);
                } else {
                    temp.push_back((addr, ep));
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
            for (addr, ep) in self.available.drain(..) {
                if dsts.contains_key(&addr) {
                    temp.push_back((addr, ep));
                } else if ep.is_idle() {
                    drop(ep);
                } else {
                    // self.pending_connections -= ep.connecting.len();
                    // self.established_connections -= ep.connected.len();
                    self.retired.insert(addr, ep);
                }
            }
            for _ in 0..temp.len() {
                let (addr, ep) = temp.pop_front().unwrap();
                self.available.insert(addr, ep);
            }
        }

        // Add new endpoints or update the base weights of existing endpoints.
        let name = self.dst_name.clone();
        for (addr, weight) in dsts.drain(..) {
            self.available
                .entry(addr)
                .or_insert_with(|| Endpoint::new(name.clone(), addr, weight))
                .set_weight(weight);
        }
    }
}

pub struct Managing {
    manager: Manager,
    resolution: Resolve,
}

impl Managing {
    /// Polls
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
            self.manager.update_resolved(resolution);
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
