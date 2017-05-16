//! A TCP/TLS load balancer.
//!
//! Copyright 2017 Buoyant, Inc.

#![deny(missing_docs)]

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
//extern crate twox_hash;
extern crate url;

mod connection;
mod duplex;
mod lb;
mod path;
mod proxy_stream;
mod resolver;
mod router;
mod server;
mod socket;

use connection::Connection;
use lb::{DstAddr, DstConnection};
use path::Path;
use socket::Socket;

/// Describes a configuratin error.
#[derive(Clone, Debug)]
pub struct ConfigError(String);

impl<'a> From<&'a str> for ConfigError {
    fn from(msg: &'a str) -> ConfigError {
        ConfigError(msg.into())
    }
}

impl<'a> From<String> for ConfigError {
    fn from(msg: String) -> ConfigError {
        ConfigError(msg)
    }
}

impl ::std::fmt::Display for ConfigError {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> Result<(), ::std::fmt::Error> {
        fmt.write_str(&self.0)
    }
}
