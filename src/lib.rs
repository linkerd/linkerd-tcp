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

use std::iter::Iterator;
use std::net;
use std::rc::Rc;

mod namerd;

#[derive(Clone, Debug)]
pub struct WeightedAddr(pub net::SocketAddr, pub f32);

pub type PathElem = Rc<Vec<u8>>;
pub type PathElems = Rc<Vec<PathElem>>;

#[derive(Clone, Debug)]
pub struct Path(PathElems);
impl Path {
    pub fn empty() -> Path { Path(Rc::new(Vec::new())) }

    pub fn new(elems0: Vec<Vec<u8>>) -> Path {
        let mut elems = Vec::with_capacity(elems0.len());
        for el in elems0 {
            elems.push(Rc::new(el));
        }
        Path(Rc::new(elems))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn elems(&self) -> PathElems {
        self.0.clone()
    }

    pub fn concat(&self, other: Path) -> Path {
        if self.len() == 0 {
            other.clone()
        } else if other.len() == 0 {
            self.clone()
        } else {
            let mut elems = Vec::with_capacity(self.len() + other.len());
            for el in &*self.0 {
                elems.push(el.clone());
            }
            for el in &*other.0 {
                elems.push(el.clone());
            }
            Path(Rc::new(elems))
        }
    } 
}

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

pub type EnvelopedConnectionStream = futures::Stream<Item = EnvelopedConnection,
                                                     Error = std::io::Error> + 'static;
                                                     