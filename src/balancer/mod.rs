use super::Path;
use super::connector::Connector;
use super::resolver::Resolve;
use futures::{Async, Future, Poll, Sink, Stream, unsync};
use ordermap::OrderMap;
use std::{cmp, io, net};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

mod dispatcher;
mod endpoint;
mod factory;
mod updater;

pub use self::endpoint::{Connection as EndpointConnection, Ctx as EndpointCtx};
use self::endpoint::Endpoint;
pub use self::factory::BalancerFactory;
pub use self::updater::Updater;

type Waiter = unsync::oneshot::Sender<endpoint::Connection>;

/// A weighted concrete destination address.
#[derive(Clone, Debug)]
pub struct WeightedAddr {
    pub addr: ::std::net::SocketAddr,
    pub weight: f32,
}

impl WeightedAddr {
    pub fn new(addr: net::SocketAddr, weight: f32) -> WeightedAddr {
        WeightedAddr { addr, weight }
    }
}

pub fn new(reactor: &Handle,
           timer: &Timer,
           dst: &Path,
           min_conns: usize,
           max_waiters: usize,
           connector: Connector,
           resolve: Resolve,
           metrics: &tacho::Scope)
           -> Balancer {
    let metrics = metrics.clone().prefixed("balancer");
    let (tx, rx) = unsync::mpsc::unbounded();
    let endpoints = Rc::new(RefCell::new(Endpoints::default()));

    let updater = {
        let metrics = metrics.clone().prefixed("endpoints");
        updater::new(dst.clone(),
                     endpoints.clone(),
                     metrics.gauge("available"),
                     metrics.gauge("retired"))
    };
    let update = resolve.filter_map(|res| res.ok()).forward(updater);
    reactor.spawn(update.map(|_| {}));

    let dispatcher = dispatcher::new(reactor.clone(),
                                     timer.clone(),
                                     connector,
                                     endpoints,
                                     min_conns,
                                     max_waiters,
                                     &metrics);
    let dispatch = rx.forward(dispatcher.sink_map_err(|_| {}));
    reactor.spawn(dispatch.map(|_| {}));

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
        let mut recv = self.0
            .take()
            .expect("connect must not be polled after completion")?;
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

#[derive(Default)]
pub struct Endpoints {
    //minimum_connections: usize,
    /// Endpoints considered available for new connections.
    available: EndpointMap,

    /// Endpoints that are still actvie but considered unavailable for new connections.
    retired: EndpointMap,
}

impl Endpoints {
    pub fn available(&self) -> &OrderMap<net::SocketAddr, Endpoint> {
        &self.available
    }

    // pub fn retired(&self) -> &OrderMap<net::SocketAddr, Endpoint> {
    //     &self.retired
    // }

    // TODO: we need to do some sort of probation deal to manage endpoints that are
    // retired.
    pub fn update_resolved(&mut self, dst_name: &Path, resolved: &[WeightedAddr]) {
        let mut temp = {
            let sz = cmp::max(self.available.len(), self.retired.len());
            VecDeque::with_capacity(sz)
        };
        let dsts = Endpoints::dsts_by_addr(resolved);
        self.check_retired(&dsts, &mut temp);
        self.check_available(&dsts, &mut temp);
        self.update_available_from_new(dst_name, dsts);
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

    fn update_available_from_new(&mut self,
                                 dst_name: &Path,
                                 mut dsts: OrderMap<net::SocketAddr, f32>) {
        // Add new endpoints or update the base weights of existing endpoints.
        //let metrics = self.endpoint_metrics.clone();
        for (addr, weight) in dsts.drain(..) {
            self.available
                .entry(addr)
                .or_insert_with(|| endpoint::new(dst_name.clone(), addr, weight))
                .set_weight(weight);
        }
    }

    fn dsts_by_addr(dsts: &[WeightedAddr]) -> OrderMap<net::SocketAddr, f32> {
        let mut by_addr = OrderMap::with_capacity(dsts.len());
        for &WeightedAddr { addr, weight } in dsts {
            by_addr.insert(addr, weight);
        }
        by_addr
    }
}
