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
// #[macro_use]
extern crate tokio_io;
extern crate tokio_timer;
extern crate url;

use futures::Stream;
use std::net;

pub mod namerd;
mod path;

pub use path::{PathElem, PathElems, Path};

#[derive(Clone, Debug)]
pub struct WeightedAddr(pub net::SocketAddr, pub f32);

/// Describes an incoming conneciton.
#[derive(Clone)]
pub struct Envelope {
    srv_addr: net::SocketAddr,
    src_addr: net::SocketAddr,
    src_id: Option<Path>,
    dst_service: Path,

    connect_deadline: Option<std::time::Instant>,
    stream_deadline: Option<std::time::Instant>,
    idle_timeout: Option<std::time::Duration>,
}

// TODO a r/w stream
pub enum Socket {}

pub struct EnvelopedConnection {
    pub envelope: Envelope,
    pub socket: Socket,
}

pub type EnvelopedConnectionStream =
    Stream<Item = EnvelopedConnection, Error = std::io::Error> + 'static;
