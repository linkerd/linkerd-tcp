use super::Path;
use super::connector::Connector;
use super::resolver::Resolve;
use futures::{Async, Future, Stream, Poll, unsync};
use std::{io, net};
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

mod dispatcher;
mod dispatchq;
mod endpoint;
mod endpoints;
mod factory;

pub use self::endpoint::{Connection as EndpointConnection, Ctx as EndpointCtx};
use self::endpoints::{EndpointMap, Endpoints};
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
    let endpoints = Endpoints::new(
        dst.clone(),
        resolve,
        connector.failure_limit(),
        connector.failure_penalty(),
        &metrics.clone().prefixed("endpoint"),
    );
    let dispatcher = dispatcher::new(
        reactor.clone(),
        timer.clone(),
        connector,
        endpoints,
        metrics,
    );
    reactor.spawn(rx.forward(dispatcher).map(|_| {}));
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
