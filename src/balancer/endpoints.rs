use super::endpoint::{self, Endpoint};
use super::super::{Path, WeightedAddr};
use super::super::resolver::Resolve;
use futures::{Async, Stream};
use ordermap::OrderMap;
use std::{cmp, net};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tacho;

pub type EndpointMap = OrderMap<net::SocketAddr, Endpoint>;
pub type FailedMap = OrderMap<net::SocketAddr, (Instant, Endpoint)>;

pub struct Endpoints {
    dst_name: Path,

    fail_limit: usize,

    fail_penalty: Duration,

    resolve: Option<Resolve>,

    //minimum_connections: usize,
    /// Endpoints considered available for new connections.
    available: EndpointMap,

    /// Endpoints that are still active but considered unavailable for new connections.
    retired: EndpointMap,

    failed: FailedMap,

    metrics: Metrics,
}

impl Endpoints {
    pub fn new(
        dst_name: Path,
        resolve: Resolve,
        fail_limit: usize,
        fail_penalty: Duration,
        metrics: &tacho::Scope,
    ) -> Endpoints {
        Endpoints {
            dst_name,
            fail_limit,
            fail_penalty,
            resolve: Some(resolve),
            available: EndpointMap::default(),
            retired: EndpointMap::default(),
            failed: FailedMap::default(),
            metrics: Metrics::new(metrics),
        }
    }

    pub fn updated_available(&mut self) -> &EndpointMap {
        if let Some(addrs) = self.poll_resolve() {
            self.update_resolved(&addrs);
            debug!(
                "balancer updated: available={} failed={}, retired={}",
                self.available.len(),
                self.failed.len(),
                self.retired.len()
            );
        }

        self.update_failed();
        self.record();
        &self.available
    }

    fn poll_resolve(&mut self) -> Option<Vec<WeightedAddr>> {
        // Poll the resolution until it's
        let mut addrs = None;
        if let Some(mut resolve) = self.resolve.take() {
            loop {
                match resolve.poll() {
                    Ok(Async::NotReady) => {
                        self.resolve = Some(resolve);
                        break;
                    }
                    Ok(Async::Ready(None)) => {
                        error!(
                            "{}: resolution complete! no further updates will be received",
                            self.dst_name
                        );
                        break;
                    }
                    //
                    Err(e) => {
                        error!("{}: resolver error: {:?}", self.dst_name, e);
                    }
                    Ok(Async::Ready(Some(Err(e)))) => {
                        error!("{}: resolver error: {:?}", self.dst_name, e);
                    }
                    Ok(Async::Ready(Some(Ok(a)))) => {
                        addrs = Some(a);
                    }
                }
            }
        }
        addrs
    }

    fn update_failed(&mut self) {
        let mut available = VecDeque::with_capacity(self.failed.len());
        let mut failed = VecDeque::with_capacity(self.failed.len());

        for (_, ep) in self.available.drain(..) {
            if ep.state().consecutive_failures < self.fail_limit {
                available.push_back(ep);
            } else {
                failed.push_back((Instant::now(), ep));
            }
        }

        for (_, (start, ep)) in self.failed.drain(..) {
            if start + self.fail_penalty <= Instant::now() {
                available.push_back(ep);
            } else {
                failed.push_back((start, ep));
            }
        }

        if available.is_empty() {
            while let Some((_, ep)) = failed.pop_front() {
                self.available.insert(ep.peer_addr(), ep);
            }
        } else {
            while let Some(ep) = available.pop_front() {
                self.available.insert(ep.peer_addr(), ep);
            }
            while let Some((since, ep)) = failed.pop_front() {
                self.failed.insert(ep.peer_addr(), (since, ep));
            }
        }
    }

    // TODO: we need to do some sort of probation deal to manage endpoints that are
    // retired.
    fn update_resolved(&mut self, resolved: &[WeightedAddr]) {
        let mut temp = {
            let sz = cmp::max(self.available.len(), self.retired.len());
            VecDeque::with_capacity(sz)
        };
        let dsts = Endpoints::dsts_by_addr(resolved);
        self.check_retired(&dsts, &mut temp);
        self.check_available(&dsts, &mut temp);
        self.check_failed(&dsts);
        self.update_available_from_new(dsts);
    }

    /// Checks active endpoints.
    fn check_available(
        &mut self,
        dsts: &OrderMap<net::SocketAddr, f64>,
        temp: &mut VecDeque<Endpoint>,
    ) {
        for (addr, ep) in self.available.drain(..) {
            if dsts.contains_key(&addr) {
                temp.push_back(ep);
            } else if ep.is_idle() {
                drop(ep);
            } else {
                self.retired.insert(addr, ep);
            }
        }

        for _ in 0..temp.len() {
            let ep = temp.pop_front().unwrap();
            self.available.insert(ep.peer_addr(), ep);
        }
    }

    /// Checks retired endpoints.
    ///
    /// Endpoints are either salvaged backed into the active pool, maintained as
    /// retired if still active, or dropped if inactive.
    fn check_retired(
        &mut self,
        dsts: &OrderMap<net::SocketAddr, f64>,
        temp: &mut VecDeque<Endpoint>,
    ) {
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

    /// Checks failed endpoints.
    fn check_failed(&mut self, dsts: &OrderMap<net::SocketAddr, f64>) {
        let mut temp = VecDeque::with_capacity(self.failed.len());
        for (addr, (since, ep)) in self.failed.drain(..) {
            if dsts.contains_key(&addr) {
                temp.push_back((since, ep));
            } else if ep.is_idle() {
                drop(ep);
            } else {
                self.retired.insert(addr, ep);
            }
        }

        for _ in 0..temp.len() {
            let (instant, ep) = temp.pop_front().unwrap();
            self.failed.insert(ep.peer_addr(), (instant, ep));
        }
    }

    fn update_available_from_new(&mut self, mut dsts: OrderMap<net::SocketAddr, f64>) {
        // Add new endpoints or update the base weights of existing endpoints.
        //let metrics = self.endpoint_metrics.clone();
        for (addr, weight) in dsts.drain(..) {
            if let Some(&mut (_, ref mut ep)) = self.failed.get_mut(&addr) {
                ep.set_weight(weight);
                continue;
            }

            if let Some(mut ep) = self.available.get_mut(&addr) {
                ep.set_weight(weight);
                continue;
            }

            self.available.insert(addr, endpoint::new(addr, weight));
        }
    }

    fn dsts_by_addr(dsts: &[WeightedAddr]) -> OrderMap<net::SocketAddr, f64> {
        let mut by_addr = OrderMap::with_capacity(dsts.len());
        for &WeightedAddr { addr, weight } in dsts {
            by_addr.insert(addr, weight);
        }
        by_addr
    }

    fn record(&self) {
        let mut open = 0;
        let mut pending = 0;

        self.metrics.available.set(self.available.len());
        for ep in self.available.values() {
            let state = ep.state();
            open += state.open_conns;
            pending += state.pending_conns;
        }

        self.metrics.failed.set(self.failed.len());
        for &(_, ref ep) in self.failed.values() {
            let state = ep.state();
            open += state.open_conns;
            pending += state.pending_conns;
        }

        self.metrics.retired.set(self.retired.len());
        for ep in self.retired.values() {
            let state = ep.state();
            open += state.open_conns;
            pending += state.pending_conns;
        }

        self.metrics.open.set(open);
        self.metrics.pending.set(pending);
    }
}


struct Metrics {
    available: tacho::Gauge,
    failed: tacho::Gauge,
    retired: tacho::Gauge,
    pending: tacho::Gauge,
    open: tacho::Gauge,
}

impl Metrics {
    fn new(base: &tacho::Scope) -> Metrics {
        let ep = base.clone().prefixed("endpoint");
        let conn = base.clone().prefixed("connection");
        Metrics {
            available: ep.gauge("available"),
            failed: ep.gauge("failed"),
            retired: ep.gauge("retired"),
            pending: conn.gauge("pending"),
            open: conn.gauge("open"),
        }
    }
}
