//! A simple Layer 4 proxy. Currently TCP only.

use std::cell::RefCell;
use std::rc::Rc;
use std::env;
use std::io;
use std::net::SocketAddr;

#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate futures;
use futures::{Future, Stream};

extern crate tokio_core;
use tokio_core::reactor::Core;
use tokio_core::net::{TcpListener, TcpStream};

extern crate galadriel;
use galadriel::transfer::BufferedTransfer;

const WINDOW_SIZE: usize = 64 * 1024;

fn main() {
    drop(env_logger::init());

    // The source addr for clients to connect to. we can either get it
    // from the environment (ARGV[1]) or use a default
    let listen_addr = parse_addr(env::args().nth(1).unwrap_or("0.0.0.0:7575".to_string()));
    let down_addr = parse_addr(env::args().nth(2).unwrap_or("127.0.0.1:8080".to_string()));

    // Create the event loop and TCP listener we'll accept connections on.
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let proxy = {
        let buffer = Rc::new(RefCell::new(vec![0; WINDOW_SIZE]));

        let listener = TcpListener::bind(&listen_addr, &handle).unwrap();
        info!("Listening on {}", listen_addr);

        listener.incoming().for_each(move |(up_stream, up_addr)| {
            debug!("Proxying {} to {}", up_addr, down_addr);
            let connect = TcpStream::connect(&down_addr, &handle);
            let tx = {
                let buffer = buffer.clone();
                connect.and_then(|down_stream| transmit_duplex(up_stream, down_stream, buffer))
            };
            handle.spawn(tx.then(move |res| {
                match res {
                    Err(e) => error!("Error proxying {} to {}: {}", up_addr, down_addr, e),
                    Ok((d, u)) => {
                        debug!("Proxied {} to {}: down={}B up={}B",
                               up_addr,
                               down_addr,
                               d,
                               u)
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

fn parse_addr(a: String) -> SocketAddr {
    match a.parse() {
        Err(err) => panic!("unable to parse '{}' due to: {}.", a, err),
        Ok(addr) => addr,
    }
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
