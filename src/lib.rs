//! A TCP/TLS load balancer.
//!
//!
//!
//! Copyright 2017 Buoyant, Inc.

extern crate bytes;
#[macro_use]
extern crate log;
extern crate futures;
extern crate hyper;
extern crate rand;
extern crate rustls;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde_yaml;
extern crate tacho;
extern crate tokio_core;
#[macro_use]
extern crate tokio_io;
extern crate tokio_timer;
extern crate url;

use futures::{Future, Sink, Stream, Poll, Async, StartSend, AsyncSink};
use std::{io, net};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use tokio_core::reactor::{Handle, Remote};

mod connection;
use connection::{Connection, Duplex, Envelope, Summary};

pub mod namerd;

mod path;
pub use path::{PathElem, PathElems, Path};

mod proxy_stream;
use proxy_stream::ProxyStream;

mod server;

mod socket;
use socket::Socket;

#[derive(Clone, Debug)]
pub struct DstAddr {
    pub addr: net::SocketAddr,
    pub weight: f32,
}

/// A weighted concrete destination address.
impl DstAddr {
    fn new(addr: net::SocketAddr, weight: f32) -> DstAddr {
        DstAddr {
            addr: addr,
            weight: weight,
        }
    }
}

/// Routes
#[derive(Clone)]
pub struct Router {
    reactor: Handle,
    routes: Rc<RefCell<HashMap<Path, Route>>>,
    resolver: Resolver,
}

impl Router {
    pub fn route(&mut self, env: &Envelope) -> Route {
        let mut routes = self.routes.borrow_mut();

        // Try to get a balancer from the cache.
        if let Some(bal) = routes.get(&env.dst_name) {
            return (*bal).clone();
        }

        let resolve = self.resolver.resolve(&env.dst_name);
        let route = Route {
            reactor: self.reactor.clone(),
            resolve: Some(resolve),
        };
        routes.insert(env.dst_name.clone(), route.clone());
        route
    }
}

// TODO In the future, we likely want to change this to use the split bind & addr APIs so
// that.
#[derive(Clone)]
struct Resolver {
    reactor: Remote,
}
impl Resolver {
    pub fn resolve(&mut self, path: Path) -> Resolve {
        Resolve { path: path }
    }
}

// A stream for a name resolution
#[derive(Clone, Debug)]
struct Resolve {
    path: Path,
    resolution: Option<Vec<DstAddr>>,
}
impl Stream for Resolve {
    type Item = Vec<DstAddr>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}

/// Materializes a load balancer from a resolution stream.
///
///
#[derive(Clone)]
pub struct Route {
    reactor: Handle,
    resolve: Option<Resolve>,
}
impl Future for Route {
    type Item = Balancer;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.resolve.take() {
            None => panic!("polled after completion"),
            Some(mut resolve) => {
                match resolve.poll()? {
                    Async::Ready(None) => {
                        Err(io::Error::new(io::ErrorKind::Other,
                                           "resolution stream ended prematurely"))
                    }
                    Async::NotReady => {
                        self.resolve = Some(resolve);
                        Ok(Async::NotReady)
                    }
                    Async::Ready(Some(addr)) => {
                        let bal = Balancer { addrs: Rc::new(RefCell::new(addr)) };

                        let updating = resolve.forward(bal.clone()).map(|_| {}).map_err(|_| {});
                        self.reactor.spawn(updating);

                        Ok(Async::Ready(bal))
                    }
                }
            }
        }
    }
}

/// Balancers accept a stream of service discovery updates and
///
///
#[derive(Clone)]
pub struct Balancer {
    addrs: Rc<RefCell<Vec<DstAddr>>>,
}
impl Balancer {
    pub fn connect(&mut self) -> Connect {
        unimplemented!()
    }
}
impl Sink for Balancer {
    type SinkItem = Vec<DstAddr>;
    type SinkError = io::Error;

    /// Update the load balancer from service discovery.
    fn start_send(&mut self, new_addrs: Vec<DstAddr>) -> StartSend<Vec<DstAddr>, Self::SinkError> {
        // TODO this is where we will update the load balancer's state to retire lapsed
        // endpoints.  Should new connections be initiated here as well?
        let mut addrs = self.addrs.borrow_mut();
        *addrs = new_addrs;

        Ok(AsyncSink::Ready)
    }

    /// Never completes.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::NotReady)
    }
}

pub struct Connect();
impl Future for Connect {
    type Item = Connection;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        unimplemented!()
    }
}
