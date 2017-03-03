//! A simple Layer 4 proxy. Currently TCP only.

#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate futures;
extern crate linkerd_tcp;
extern crate tokio_core;

use clap::{Arg, App};
use futures::{Future, Stream};
use std::cell::RefCell;
use std::rc::Rc;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio_core::reactor::Core;
use tokio_core::net::{TcpListener, TcpStream};

use linkerd_tcp::{BufferedTransfer, Endpointer, WithAddr};
use linkerd_tcp::namerd;

const WINDOW_SIZE: usize = 64 * 1024;

static LISTEN_ADDR_OPT: &'static str = "listen-addr";
static NAMERD_ADDR_OPT: &'static str = "namerd-addr";
static NAMERD_NS_OPT: &'static str = "namerd-ns";
static NAMERD_INTERVAL_OPT: &'static str = "namerd-interval";
static TARGET_OPT: &'static str = "TARGET";

fn main() {
    // Configure the logger from the RUST_LOG environment variable.
    drop(env_logger::init());

    // Parse and load command-line options.
    let opts = mk_app().get_matches();
    let listen_addr = opts.value_of(LISTEN_ADDR_OPT).unwrap().parse().unwrap();
    let namerd_addr = opts.value_of(NAMERD_ADDR_OPT).unwrap().parse().unwrap();
    let namerd_ns = opts.value_of(NAMERD_NS_OPT).unwrap();
    let namerd_interval = {
        let i = opts.value_of(NAMERD_INTERVAL_OPT).unwrap().parse().unwrap();
        Duration::from_secs(i)
    };
    let target_path = opts.value_of(TARGET_OPT).unwrap();

    // Currently, we poll namerd to resolve a downstream name. This is
    // done in a dedicated thread with blocking I/O and sleep. Namerd
    // exposes a P2C Weighted-Least-Connections balancer.
    //
    // This probably should be refactored a bit to separate the namerd
    // polling from balancing logic.

    let balancer = namerd::Endpointer::periodic(&namerd_addr,
                                                namerd_interval,
                                                namerd_ns.to_string(),
                                                target_path.to_string());
    info!("Updating {} from {} every {}s",
          target_path,
          namerd_addr,
          namerd_interval.as_secs());

    // Listen on a socket. For each connection accepted, ask the
    // balancer for a downstream endpoint. Initiate a connection to
    // that endpoint and proxy all data between the upstream and
    // downstream connections, copying data into a temporary buffer.
    //
    // All of this happens on the main thread, via tokio's Core
    // reactor.

    let mut core = Core::new().unwrap();
    let proxy = {
        let handle = core.handle();
        let listener = TcpListener::bind(&listen_addr, &handle).unwrap();
        info!("Listening on {}", listen_addr);

        let buffer = Rc::new(RefCell::new(vec![0; WINDOW_SIZE]));
        listener.incoming().for_each(move |(up_stream, up_addr)| {
            match balancer.endpoint() {
                None => error!("No endpoints are available"),
                Some(down_endpoint) => {
                    // TODO retry on failed connections...
                    let down_addr = down_endpoint.addr();
                    // debug!("Proxying {} to {}", up_addr, down_addr);
                    let connect = TcpStream::connect(&down_addr, &handle);
                    let tx = {
                        let buffer = buffer.clone();
                        connect.and_then(|down_stream| {
                            transmit_duplex(up_stream, down_stream, buffer)
                        })
                    };
                    handle.spawn(tx.then(move |res| {
                        match res {
                            Err(e) => error!("Error proxying {} to {}: {}", up_addr, down_addr, e),
                            Ok((down_bytes, up_bytes)) => {
                                drop(down_endpoint); // make sure this is moved into this scope.
                                debug!("proxied {} to {}: down={}B up={}B",
                                       up_addr,
                                       down_addr,
                                       down_bytes,
                                       up_bytes)
                            }
                        };
                        Ok(())
                    }));
                }
            };
            Ok(())
        })
    };

    core.run(proxy).unwrap();
}

fn mk_app() -> App<'static, 'static> {
    App::new(crate_name!())
        .version(crate_version!())
        .about(crate_description!())
        .arg(Arg::with_name(LISTEN_ADDR_OPT)
            .short("l")
            .long(LISTEN_ADDR_OPT)
            .default_value("0.0.0.0:7575")
            .takes_value(true)
            .value_name("ADDR")
            .validator(is_socket_addr)
            .help("Accept connections on the given local address and port"))
        .arg(Arg::with_name(NAMERD_ADDR_OPT)
            .short("n")
            .long(NAMERD_ADDR_OPT)
            .default_value("127.0.0.1:4180")
            .takes_value(true)
            .value_name("ADDR")
            .validator(is_socket_addr)
            .help("The address of namerd's HTTP interface"))
        .arg(Arg::with_name(NAMERD_NS_OPT)
            .short("N")
            .long(NAMERD_NS_OPT)
            .default_value("default")
            .takes_value(true)
            .value_name("NS")
            .help("Namerd namespace in which the target will be resolved"))
        .arg(Arg::with_name(NAMERD_INTERVAL_OPT)
            .short("i")
            .long(NAMERD_INTERVAL_OPT)
            .default_value("60")
            .takes_value(true)
            .value_name("SECS")
            .help("Namerd refresh interval in seconds"))
        .arg(Arg::with_name(TARGET_OPT)
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

fn transmit_duplex(up_stream: TcpStream,
                   down_stream: TcpStream,
                   buffer: Rc<RefCell<Vec<u8>>>)
                   -> Box<Future<Item = (u64, u64), Error = io::Error>> {
    let up = Rc::new(up_stream);
    let down = Rc::new(down_stream);
    let down_tx = BufferedTransfer::new(up.clone(), down.clone(), buffer.clone());
    let up_tx = BufferedTransfer::new(down, up, buffer.clone());
    Box::new(down_tx.join(up_tx))
}
