use super::Path;
use super::connection::Ctx;
use super::connector::Connector;
use futures::unsync::{mpsc, oneshot};
use std::net;
use tokio_core::reactor::Handle;

mod dispatcher;
mod factory;
mod manager;

pub use self::dispatcher::{Dispatcher, Dispatch};
//use self::endpoint::Endpoint;
//pub use self::endpoint::EndpointCtx;
pub use self::factory::BalancerFactory;
pub use self::manager::{Manager, Managing, EndpointCtx};
//use self::pool::Pool;

pub type DstConnection = super::Connection<DstCtx>;

pub enum Error {
    ResolverLost(),
}

/// A weighted concrete destination address.
#[derive(Clone, Debug)]
pub struct DstAddr {
    pub addr: ::std::net::SocketAddr,
    pub weight: f32,
}

impl DstAddr {
    pub fn new(addr: ::std::net::SocketAddr, weight: f32) -> DstAddr {
        DstAddr {
            addr: addr,
            weight: weight,
        }
    }
}

pub struct Balancer {
    pub manager: Manager,
    pub dispatcher: Dispatcher,
}

impl Balancer {
    pub fn new(reactor: Handle, dst: Path, min_conns: usize, conn: Connector) -> Balancer {
        let (tx, rx) = mpsc::unbounded();
        Balancer {
            manager: manager::new(dst, reactor, conn, min_conns, rx),
            dispatcher: dispatcher::new(tx),
        }
    }
}

struct Summary {
    _name: Path,
    local_addr: net::SocketAddr,
    peer_addr: net::SocketAddr,
    read_count: usize,
    read_bytes: usize,
    write_count: usize,
    write_bytes: usize,
}

pub struct DstCtx {
    summary: Option<Summary>,
    tx: Option<oneshot::Sender<Summary>>,
}

impl DstCtx {
    fn new(name: Path,
           local_addr: net::SocketAddr,
           peer_addr: net::SocketAddr,
           tx: oneshot::Sender<Summary>)
           -> DstCtx {
        DstCtx {
            tx: Some(tx),
            summary: Some(Summary {
                              _name: name,
                              local_addr: local_addr,
                              peer_addr: peer_addr,
                              read_count: 0,
                              read_bytes: 0,
                              write_count: 0,
                              write_bytes: 0,
                          }),
        }
    }
}

impl Ctx for DstCtx {
    fn local_addr(&self) -> net::SocketAddr {
        self.summary.as_ref().unwrap().local_addr
    }

    fn peer_addr(&self) -> net::SocketAddr {
        self.summary.as_ref().unwrap().peer_addr
    }

    fn read(&mut self, sz: usize) {
        if let Some(mut summary) = self.summary.as_mut() {
            summary.read_count += 1;
            summary.read_bytes += sz;
        }
    }

    fn wrote(&mut self, sz: usize) {
        if let Some(mut summary) = self.summary.as_mut() {
            summary.write_count += 1;
            summary.write_bytes += sz;
        }
    }
}

impl Drop for DstCtx {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            if let Some(summary) = self.summary.take() {
                let _ = tx.send(summary);
            }
        }
    }
}
