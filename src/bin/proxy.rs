//! A simple Layer 3 proxy. Currently TCP only.

extern crate tokio_core;
extern crate futures;

use std::env;
use std::io::Error;

use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::Core;
use tokio_core::io::{copy, Io};

use futures::stream::Stream;
use futures::{done, Future};

fn main() {
    // The source addr for clients to connect to.
    let addr = env::args().nth(1).unwrap_or("127.0.0.1:7575".to_string());
    let addr = addr.parse().unwrap();

    // The target addr to proxy connections to.
    let target_addr = env::args().nth(2).unwrap_or("127.0.0.1:8080".to_string());
    let target_addr = target_addr.parse().unwrap();

    // Create the event loop and TCP listener we'll accept connections on.
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let socket = TcpListener::bind(&addr, &handle).unwrap();

    println!("Listening on: {}", addr);

    let srv = socket.incoming().for_each(|(ingress, addr)| {
        println!("New Connection: {}", addr);
        let (from_ingress, to_ingress) = ingress.split();
        let handler = TcpStream::connect(&target_addr, &handle).and_then(|egress| {
            let (from_egress, to_egress) = egress.split();

            // By using `copy`, we don't support connection re-use well.
            // Better would be to write our own framing on top of `read_until`
            copy(from_ingress, to_egress)
                .and_then(|size| {
                    println!("copied {} bytes from_ingress -> to_egress", size);
                    copy(from_egress, to_ingress)
                })
                .and_then(|size| {
                    println!("copied {} bytes from_egress -> to_ingress", size);
                    done::<u64, Error>(Ok(size))
                })
        });
        // Handle proxying concurrently.
        handle.spawn(handler.then(|_| Ok(())));
        Ok(())
    });

    // execute server
    core.run(srv).unwrap();
}
