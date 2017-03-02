//! A simple Layer 4 proxy. Currently TCP only.

#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate futures;
extern crate galadriel;
extern crate tokio_core;

use clap::{Arg, App};
use futures::{Future, Stream};
use std::cell::RefCell;
use std::rc::Rc;
use std::io::{self, Write};
use std::process;
use std::time::Duration;
use tokio_core::reactor::Core;
use tokio_core::net::{TcpListener, TcpStream};

use galadriel::{BufferedTransfer, Endpointer, WithAddr};
use galadriel::namerd;

const WINDOW_SIZE: usize = 64 * 1024;

fn stderr(msg: String) {
    let _ = writeln!(&mut io::stderr(), "{}", msg);
}

fn main() {
    drop(env_logger::init());

    let tcpd_app = App::new("tcpd")
        .version(crate_version!())
        .about(crate_description!())
        .author(crate_authors!("\n"))
        .arg(Arg::with_name("listen-addr")
            .short("l")
            .long("listen-addr")
            .default_value("0.0.0.0:7575")
            .takes_value(true)
            .value_name("ADDR")
            .help("Accept connections on the given local address and port"))
        .arg(Arg::with_name("namerd-addr")
            .short("n")
            .long("namerd-addr")
            .default_value("127.0.0.1:4180")
            .takes_value(true)
            .value_name("ADDR")
            .help("The address of namerd's HTTP interface"))
        .arg(Arg::with_name("namerd-ns")
            .short("N")
            .long("namerd-ns")
            .default_value("default")
            .takes_value(true)
            .value_name("NS")
            .help("Namerd namespace in which the target will be resolved"))
        .arg(Arg::with_name("namerd-interval")
            .short("i")
            .long("namerd-interval")
            .default_value("60")
            .takes_value(true)
            .value_name("SECS")
            .help("Namerd refresh interval in seconds"))
        .arg(Arg::with_name("TARGET")
            .required(true)
            .index(1)
            .help("Destination name (e.g. /svc/foo)"));

    let opts = tcpd_app.get_matches();
    let listen_addr = opts.value_of("listen-addr").unwrap().parse().unwrap();
    let namerd_addr = opts.value_of("namerd-addr").unwrap().parse().unwrap();
    let namerd_ns = opts.value_of("namerd-ns").unwrap();
    let namerd_interval = {
        let i = opts.value_of("namerd-interval").unwrap().parse().unwrap();
        Duration::from_secs(i)
    };

    let target_path = opts.value_of("TARGET").unwrap();
    if !target_path.starts_with("/") {
        stderr(format!("TARGET not a /path: {}", target_path));
        process::exit(64); // EX_USAGE
    }

    // Create the event loop and TCP listener we'll accept connections on.
    let mut core = Core::new().unwrap();

    let proxy = {
        let handle = core.handle();
        let listener = TcpListener::bind(&listen_addr, &handle).unwrap();
        info!("Listening on {}", listen_addr);

        let namerd = namerd::Endpointer::periodic(&namerd_addr,
                                                  namerd_interval,
                                                  namerd_ns.to_string(),
                                                  target_path.to_string());
        info!("Updating {} from {} every {}s",
              target_path,
              namerd_addr,
              namerd_interval.as_secs());

        let buffer = Rc::new(RefCell::new(vec![0; WINDOW_SIZE]));
        listener.incoming().for_each(move |(up_stream, up_addr)| {
            let down_endpoint = namerd.endpoint();
            let down_addr = down_endpoint.addr();

            // debug!("Proxying {} to {}", up_addr, down_addr);
            let connect = TcpStream::connect(&down_addr, &handle);
            let tx = {
                let buffer = buffer.clone();
                connect.and_then(|down_stream| transmit_duplex(up_stream, down_stream, buffer))
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
            Ok(())
        })
    };

    // You can run multiple `core.run()` in multiple threads, and use
    // SO_REUSEPORT tokio does not currently support thread pool
    // executors but you can thread.spawn N times.
    core.run(proxy).unwrap();
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
