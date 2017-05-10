//! A simple layer-4 load balancing library on tokio.
//!
//! Inspired by https://github.com/tailhook/tk-pool.
//! Copyright 2016 The tk-pool Developers
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
extern crate tokio_io;
extern crate tokio_timer;
extern crate url;

use futures::{Future, Sink, Stream, Poll, Async, StartSend, AsyncSink};
use std::cell::RefCell;
use std::collections::HashMap;
use std::net;
use std::rc::Rc;
use tokio_core::reactor::{Handle, Remote};

pub mod namerd;

mod path;
pub use path::{PathElem, PathElems, Path};

/// Describes an incoming connection.
#[derive(Clone, Debug, Hash)]
pub struct SrcEnvelope {
    /// The address of the server interface on which the connection was received.
    local_addr: net::SocketAddr,

    /// The address of the client that originated the connection.
    remote_addr: net::SocketAddr,

    /// An optional identifier for the client (e.g. from TLS mutual auth.
    remote_id: Option<Path>,

    /// The destination service name to be resolved through namerd.
    dst_name: Path,

    /// The time by which the stream must be established.
    connect_deadline: Option<std::time::Instant>,

    /// The time by which the stream must be completed.
    stream_deadline: Option<std::time::Instant>,

    /// The amount of time that a stream may remain idle before being closed.
    idle_timeout: Option<std::time::Duration>,
}

// TODO a r/w stream
pub struct Socket(SocketInternal);
enum SocketInternal {
    Plain(DstEnvelope),
    Secure(DstEnvelope),
}

pub struct SrcConnection {
    pub envelope: SrcEnvelope,
    pub socket: Socket,
}

///
#[derive(Clone, Debug)]
pub struct DstAddr {
    pub addr: net::SocketAddr,
    pub weight: f32,
}
impl DstAddr {
    fn new(addr: net::SocketAddr, weight: f32) -> DstAddr {
        DstAddr {
            addr: addr,
            weight: weight,
        }
    }
}

pub struct DstEnvelope {
    pub local_addr: net::SocketAddr,
    pub remote_addr: net::SocketAddr,
}

pub struct DstConnection {
    pub envelope: DstEnvelope,
    pub socket: Socket,
}

/// Routes
#[derive(Clone)]
pub struct Router {
    reactor: Handle,
    balancers: HashMap<Path, GetBalancer>,
    resolver: Resolver,
}

impl Router {
    pub fn get_balancer(&mut self, e: &Envelope) -> GetBalancer {
        // Try to get a balancer from the cache.
        if let Some(bal) = self.balancers.get(&e.dst_name) {
            return (*bal).clone();
        }

        //
        let bal = GetBalancer {
            reactor: self.reactor.clone(),
            resolve: Some(self.resolver.resolve(&e.dst_name)),
        };
        self.balancers.insert(e.dst_name.clone(), bal.clone());
        bal
    }
}

// TODO In the future, we likely want to change this to use the split bind & addr APIs so
// that.
#[derive(Clone)]
struct Resolver {
    reactor: Remote,
}
impl Resolver {
    pub fn resolve(&mut self, envelope: SrcEnvelope) -> Resolve {
        unimplemented!()
    }
}

// A stream for a name resolution
#[derive(Clone, Debug)]
struct Resolve {
    src: SrcEnvelope,
    resolution: Option<Vec<DstAddr>>,
}
impl Stream for Resolve {
    type Item = Vec<DstAddr>;
    type Error = ::std::io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}

///
#[derive(Clone)]
pub struct GetBalancer {
    reactor: Handle,
    resolve: Option<Resolve>,
}

impl Future for GetBalancer {
    type Item = Balancer;
    type Error = ::std::io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.resolve.take() {
            None => panic!("polled after completion"),
            Some(mut resolve) => {
                match resolve.poll() {
                    Err(e) => Err(e),
                    Ok(Async::Ready(None)) => {
                        Err(::std::io::Error::new(::std::io::ErrorKind::Other,
                                                  "resolution stream ended prematurely"))
                    }
                    Ok(Async::Ready(Some(addr))) => {
                        let bal = Balancer { advertised: Rc::new(RefCell::new(addr)) };
                        let updating = resolve.forward(bal.clone()).map(|_| {}).map_err(|_| {});
                        self.reactor.spawn(updating);
                        Ok(Async::Ready(bal))
                    }
                    Ok(Async::NotReady) => {
                        self.resolve = Some(resolve);
                        Ok(Async::NotReady)
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
    advertised: Rc<RefCell<Vec<DstAddr>>>,
}

impl Balancer {
    pub fn connect(&mut self) -> DstConnection {
        unimplemented!()
    }
}

impl Sink for Balancer {
    type SinkItem = Vec<DstAddr>;
    type SinkError = ::std::io::Error;

    fn start_send(&mut self, addrs: Vec<DstAddr>) -> StartSend<Vec<DstAddr>, Self::SinkError> {
        // TODO this is where we will update the load balancer's state to retire lapsed
        // endpoints.  Should new connections be initiated here as well?
        let mut advertised = self.advertised.borrow_mut();
        *advertised = addrs;

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::NotReady)
    }
}

/// ...
impl Future for Streaming {
    type Item = ();
    type Error = ::std::io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        unimplemented!()
    }
}

pub struct StreamComplete();
impl Future for Streaming {
    type Item = ();
    type Error = ::std::io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        unimplemented!()
    }
}
