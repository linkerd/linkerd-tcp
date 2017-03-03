//! A TCP proxy for the linkerd service mesh.

#[macro_use]
extern crate clap;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate futures;
extern crate linkerd_tcp;
extern crate rand;
extern crate tokio_core;

use clap::{Arg, App};
use hyper::Client;
use futures::Stream;
use std::{io, time};
use std::net::SocketAddr;
use tokio_core::reactor::Core;
use tokio_core::net::*;

use linkerd_tcp::*;

// Start a single server.
fn main() {
    // Configure the logger from the RUST_LOG environment variable.
    drop(env_logger::init());

    // Parse and load command-line options.
    let opts = mk_app().get_matches();
    let listen_addr = opts.value_of(LISTEN_ADDR_ARG)
        .unwrap()
        .parse()
        .unwrap();
    let window_size_kb = opts.value_of(WINDOW_SIZE_KB_ARG)
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let namerd_addr = opts.value_of(NAMERD_ADDR_ARG)
        .unwrap()
        .parse()
        .unwrap();
    let namerd_ns = opts.value_of(NAMERD_NS_ARG).unwrap();
    let namerd_interval = {
        let i = opts.value_of(NAMERD_INTERVAL_ARG)
            .unwrap()
            .parse()
            .unwrap();
        time::Duration::from_secs(i)
    };
    let target_path = opts.value_of(TARGET_ARG).unwrap();

    // TODO split this into two threads...

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let listener = {
        let handle = handle.clone();
        info!("Listening on {}", listen_addr);
        TcpListener::bind(&listen_addr, &handle).unwrap()
    };

    // Poll namerd to resolve a name.
    let addrs = {
        let client = Client::new(&handle.clone());
        namerd::resolve(client,
                        namerd_addr,
                        namerd_interval,
                        namerd_ns.to_string(),
                        target_path.to_string())
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "namerd error"))
    };
    info!("Updating {} from {} every {}s",
          target_path,
          namerd_addr,
          namerd_interval.as_secs());

    let balancer = {
        let handle = handle.clone();
        let bufsz = window_size_kb * 1024;
        Balancer::new(addrs, bufsz, handle.clone())
    };

    let done = listener.incoming().forward(balancer);
    core.run(done).unwrap();
}

static WINDOW_SIZE_KB_ARG: &'static str = "window-size-kb";
static LISTEN_ADDR_ARG: &'static str = "listen-addr";
static NAMERD_ADDR_ARG: &'static str = "namerd-addr";
static NAMERD_NS_ARG: &'static str = "namerd-ns";
static NAMERD_INTERVAL_ARG: &'static str = "namerd-interval";
static TARGET_ARG: &'static str = "TARGET";

fn mk_app() -> App<'static, 'static> {
    App::new(crate_name!())
        .version(crate_version!())
        .about(crate_description!())
        .arg(Arg::with_name(LISTEN_ADDR_ARG)
            .short("l")
            .long(LISTEN_ADDR_ARG)
            .default_value("0.0.0.0:7575")
            .takes_value(true)
            .value_name("ADDR")
            .validator(is_socket_addr)
            .help("Accept connections on the given local address and port"))
        .arg(Arg::with_name(NAMERD_ADDR_ARG)
            .short("n")
            .long(NAMERD_ADDR_ARG)
            .default_value("127.0.0.1:4180")
            .takes_value(true)
            .value_name("ADDR")
            .validator(is_socket_addr)
            .help("The address of namerd's HTTP interface"))
        .arg(Arg::with_name(NAMERD_NS_ARG)
            .short("N")
            .long(NAMERD_NS_ARG)
            .default_value("default")
            .takes_value(true)
            .value_name("NS")
            .help("Namerd namespace in which the target will be resolved"))
        .arg(Arg::with_name(NAMERD_INTERVAL_ARG)
            .short("i")
            .long(NAMERD_INTERVAL_ARG)
            .default_value("60")
            .takes_value(true)
            .value_name("SECS")
            .help("Namerd refresh interval in seconds"))
        .arg(Arg::with_name(WINDOW_SIZE_KB_ARG)
            .short("w")
            .long(WINDOW_SIZE_KB_ARG)
            .default_value("64")
            .takes_value(true)
            .value_name("KB")
            .validator(is_usize))
        .arg(Arg::with_name(TARGET_ARG)
            .required(true)
            .index(1)
            .validator(is_path_like)
            .help("Destination name (e.g. /svc/foo) to be resolved through namerd"))
}

fn is_path_like(v: String) -> Result<(), String> {
    if v.starts_with("/") {
        Ok(())
    } else {
        Err("The value did not start with a /".to_string())
    }
}

fn is_socket_addr(v: String) -> Result<(), String> {
    v.parse::<SocketAddr>()
        .map(|_| ())
        .map_err(|_| "The value must be an address in the form IP:PORT".to_string())
}

fn is_usize(v: String) -> Result<(), String> {
    v.parse::<usize>().map(|_| ()).map_err(|_| "The value must be a number".to_string())
}
