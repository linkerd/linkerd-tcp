//! A simple layer-4 load balancing library on tokio.
//!
//! Inspired by https://github.com/tailhook/tk-pool.
//! Copyright 2016 The tk-pool Developers
//!
//! Copyright 2017 Buoyant, Inc.

extern crate bytes;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate futures;
extern crate hyper;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde_yaml;
#[macro_use]
extern crate tokio_core;
extern crate tokio_tls;
extern crate tokio_timer;
extern crate url;

use std::net;

pub mod app;
pub mod lb;
pub use lb::Balancer;
pub mod namerd;

#[derive(Clone, Debug)]
pub struct WeightedAddr(pub net::SocketAddr, pub f32);