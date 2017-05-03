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
#[macro_use]
extern crate tokio_io;
extern crate tokio_timer;
extern crate url;

use std::net;

mod driver;
pub mod app;
pub mod lb;
pub mod namerd;

use driver::Driver;
pub use lb::Balancer;

#[derive(Clone, Debug)]
pub struct WeightedAddr(pub net::SocketAddr, pub f32);

#[derive(Clone, Debug)]
pub struct Path {
    elems: std::rc::Rc<Vec<String>>,
}

#[derive(Clone)]
pub struct IncomingEnvelope {
    srv_addr: net::SocketAddr,
    src_addr: net::SocketAddr,
    src_id: Option<Path>,
    dst_service: Path,

    connect_deadline: Option<std::time::Instant>,
    stream_deadline: Option<std::time::Instant>,
    idle_timeout: Option<std::time::Duration>,
}

// TODO a r/w stream
struct DuplexStream {}

pub struct IncomingConnection {
    envelope: IncomingEnvelope,
    stream: DuplexStream,
}

pub type Incoming = futures::Stream<Item = IncomingConnection, Error = std::io::Error> + 'static;
