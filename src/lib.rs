//! linkerd-tcp: A load-balancing TCP/TLS stream routing proxy.
//!
//!
//!
//! Copyright 2017 Buoyant, Inc.

#![deny(missing_docs)]
#![deny(warnings)]

extern crate bytes;
#[macro_use]
extern crate log;
extern crate futures;
extern crate hyper;
extern crate ordermap;
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

mod admin;
pub mod app;
mod balancer;
mod connection;
mod connector;
mod path;
mod resolver;
mod router;
mod server;

use balancer::WeightedAddr;
use path::Path;
