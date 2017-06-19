use super::Path;
use super::connector::Connector;
use super::resolver::Resolve;
use futures::{Async, Future, Poll, unsync};
use ordermap::OrderMap;
use std::{cmp, io, net};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

mod dispatcher;
mod endpoint;
mod factory;

pub use self::endpoint::{Connection as EndpointConnection, Ctx as EndpointCtx};
use self::endpoint::Endpoint;
pub use self::factory::BalancerFactory;

type Waiter = unsync::oneshot::Sender<endpoint::Connection>;

/// A weighted concrete destination address.
#[derive(Clone, Debug)]
pub struct WeightedAddr {
    pub addr: ::std::net::SocketAddr,
    pub weight: f64,
}

impl WeightedAddr {
    pub fn new(addr: net::SocketAddr, weight: f64) -> WeightedAddr {
        WeightedAddr { addr, weight }
    }
}

pub fn new(
    reactor: &Handle,
    timer: &Timer,
    dst: &Path,
    connector: Connector,
    resolve: Resolve,
    metrics: &tacho::Scope,
) -> Balancer {
    let (tx, rx) = unsync::mpsc::unbounded();
    let dispatcher = dispatcher::new(
        reactor.clone(),
        timer.clone(),
        dst.clone(),
        connector,
        resolve,
        rx,
        Endpoints::default(),
        metrics,
    );
    reactor.spawn(dispatcher.map_err(|_| {}));
    Balancer(tx)
}

#[derive(Clone)]
pub struct Balancer(unsync::mpsc::UnboundedSender<Waiter>);
impl Balancer {
    /// Obtains a connection to the destination.
    pub fn connect(&self) -> Connect {
        let (tx, rx) = unsync::oneshot::channel();
        let result = unsync::mpsc::UnboundedSender::send(&self.0, tx)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "lost dispatcher"))
            .map(|_| rx);
        Connect(Some(result))
    }
}

pub struct Connect(Option<io::Result<unsync::oneshot::Receiver<endpoint::Connection>>>);
impl Future for Connect {
    type Item = endpoint::Connection;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut recv = self.0.take().expect(
            "connect must not be polled after completion",
        )?;
        match recv.poll() {
            Err(_) => Err(io::Error::new(io::ErrorKind::Interrupted, "canceled")),
            Ok(Async::Ready(conn)) => Ok(Async::Ready(conn)),
            Ok(Async::NotReady) => {
                self.0 = Some(Ok(recv));
                Ok(Async::NotReady)
            }
        }
    }
}

pub type EndpointMap = OrderMap<net::SocketAddr, Endpoint>;
pub type FailedMap = OrderMap<net::SocketAddr, (Instant, Endpoint)>;

#[derive(Default)]
pub struct Endpoints {
    //minimum_connections: usize,
    /// Endpoints considered available for new connections.
    available: EndpointMap,

    /// Endpoints that are still active but considered unavailable for new connections.
    retired: EndpointMap,

    failed: FailedMap,
}

impl Endpoints {
    pub fn available(&self) -> &EndpointMap {
        &self.available
    }

    pub fn failed(&self) -> &FailedMap {
        &self.failed
    }

    pub fn retired(&self) -> &EndpointMap {
        &self.retired
    }

    pub fn update_failed(&mut self, max_failures: usize, penalty: Duration) {
        let mut available = VecDeque::with_capacity(self.failed.len());
        let mut failed = VecDeque::with_capacity(self.failed.len());

        for (_, ep) in self.available.drain(..) {
            if ep.state().consecutive_failures < max_failures {
                available.push_back(ep);
            } else {
                failed.push_back((Instant::now(), ep));
            }
        }

        for (_, (start, ep)) in self.failed.drain(..) {
            if start + penalty <= Instant::now() {
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
    pub fn update_resolved(&mut self, resolved: &[WeightedAddr]) {
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
}
