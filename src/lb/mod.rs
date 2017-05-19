use super::{Path, resolver};
use super::connector::Connector;
use futures::unsync::mpsc;
use ordermap::OrderMap;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use tokio_core::reactor::Handle;

mod dispatcher;
mod endpoint;
mod factory;
mod manager;
//mod pool;

pub use self::dispatcher::{Dispatcher, Dispatch};
//use self::endpoint::Endpoint;
pub use self::endpoint::EndpointCtx;
pub use self::factory::BalancerFactory;
pub use self::manager::{Manager, Managing};
//use self::pool::Pool;

pub type DstConnection = super::Connection<EndpointCtx>;

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
