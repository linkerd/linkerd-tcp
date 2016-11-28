//! A simple Layer 4 proxy. Currently TCP only.

#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate tokio_core;

use std::cell::RefCell;
use std::rc::Rc;
use std::env;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::net::Shutdown;
use std::str;

use futures::{Future, Poll, Async};
use futures::stream::Stream;
use tokio_core::reactor::Core;
use tokio_core::net::{TcpStream, TcpListener};

extern crate galadriel;
use galadriel::pool;
use galadriel::pool::{ConnectionPool, Pool};

// This works! Use it!!
fn main() {
    drop(env_logger::init());

    // The source addr for clients to connect to. we can either get it
    // from the environment (ARGV[1]) or use a default
    let addr = env::args().nth(1).unwrap_or("0.0.0.0:7575".to_string());
    let addr = addr.parse()
        .map_err(|err| {
            panic!("unable to parse '{}' due to: {}.", addr, err);
        })
        .unwrap();

    // The target addr to proxy connections to.
    let target_addr = env::args().nth(2).unwrap_or("127.0.0.1:8080".to_string());
    // we have to set this to a SocketAddr due to a type inference problem later.
    let target_addr = target_addr.parse::<SocketAddr>()
        .map_err(|err| {
            panic!("unable to parse '{}' due to {}.", target_addr, err);
        })
        .unwrap();

    // Create the event loop and TCP listener we'll accept connections on.
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    debug!("Creating a Connection Pool");
    let pool = pool::new(target_addr.clone(), handle.clone());
    // A shared buffer for a single proxy to share for ingress/egress.
    let buffer = Rc::new(RefCell::new(vec![0; 64 * 1024]));
    let listener = TcpListener::bind(&addr, &handle).unwrap();
    info!("Listening for http connections on {}", addr);
    let clients = listener.incoming().map(move |(socket, addr)| {
        (Client {
                buffer: buffer.clone(),
                // type inference fails here if we don't parse it as a SocketAddr
                addr: target_addr.clone(),
                pool: &pool,
            }
            .serve(socket),
         addr)
    });

    let handle = core.handle().clone();
    let server = clients.for_each(|(client, addr)| {
        handle.spawn(client.then(move |res| {
            match res {
                Ok((a, b)) => debug!("proxied {}/{} bytes for {}", a, b, addr),
                Err(e) => error!("error for {}: {}", addr, e),
            }
            futures::finished(())
        }));
        Ok(())
    });

    // you can run multiple `core.run()` in multiple threads, and use SO_REUSEPORT
    // tokio does not currently support thread pool executors but you can thread.spawn N times.
    core.run(server).unwrap();
}

struct Client<'a> {
    // A buffer for ingress/egress to share
    buffer: Rc<RefCell<Vec<u8>>>,
    // The remote server to connect to
    addr: SocketAddr,
    pool: &'a Pool,
}

impl<'a> Client<'a> {
    // ingress is the incoming connection to proxy
    fn serve(self, ingress: TcpStream) -> Box<Future<Item = (u64, u64), Error = io::Error>> {
        let addr = self.addr.clone();
        debug!("Getting a connection from the pool");
        let connection = self.pool.checkout();

        let connected = connection.map_err(move |err| {
                panic!("unable to connect to {} due to reason: {}", addr, err);
            })
            .and_then(move |egress| {
                // If outbound connection failure happens, it's because we're assuming success
                // on the below line
                debug!("Opening connection to egress address {}", addr);
                Ok((ingress, egress))
            });
        
        let buffer = self.buffer.clone();
        // TODO: somewhere in here we need to return the connection to the pool
        mybox(connected.and_then(move |(ingress, egress)| {
            let ingress = Rc::new(ingress);
            let egress = Rc::new(egress);

            let half1 = Transfer::new(ingress.clone(), egress.clone(), buffer.clone());
            let half2 = Transfer::new(egress, ingress, buffer);
            half1.join(half2)
        }))
    }
}

// !!! Code from here is verbatim from the tokio-socks5 example. !!!

fn mybox<F: Future + 'static>(f: F) -> Box<Future<Item = F::Item, Error = F::Error>> {
    Box::new(f)
}

/// A future representing reading all data from one side of a proxy connection
/// and writing it to another.
///
/// This future, unlike the handshake performed above, is implemented via a
/// custom implementation of the `Future` trait rather than with combinators.
/// This is intended to show off how the combinators are not all that can be
/// done with futures, but rather more custom (or optimized) implementations can
/// be implemented with just a trait impl!
struct Transfer {
    // The two I/O objects we'll be reading.
    reader: Rc<TcpStream>,
    writer: Rc<TcpStream>,

    // The shared global buffer that all connections on our server are using.
    buf: Rc<RefCell<Vec<u8>>>,

    // The number of bytes we've written so far.
    amt: u64,
}

impl Transfer {
    fn new(reader: Rc<TcpStream>, writer: Rc<TcpStream>, buffer: Rc<RefCell<Vec<u8>>>) -> Transfer {
        Transfer {
            reader: reader,
            writer: writer,
            buf: buffer,
            amt: 0,
        }
    }
}

// Here we implement the `Future` trait for `Transfer` directly. This does not
// use any combinators, and shows how you might implement it in custom
// situations if needed.
impl Future for Transfer {
    // Our future resolves to the number of bytes transferred, or an I/O error
    // that happens during the connection, if any.
    type Item = u64;
    type Error = io::Error;

    /// Attempts to drive this future to completion, checking if it's ready to
    /// be completed.
    ///
    /// This method is the core foundation of completing a future over time. It
    /// is intended to never block and return "quickly" to ensure that it
    /// doesn't block the event loop.
    ///
    /// Completion for our `Transfer` future is defined when one side hits EOF
    /// and we've written all remaining data to the other side of the
    /// connection. The behavior of `Future::poll` is in general not specified
    /// after a future resolves (e.g. in this case returns an error or how many
    /// bytes were transferred), so we don't need to maintain state beyond that
    /// point.
    fn poll(&mut self) -> Poll<u64, io::Error> {
        let mut buffer = self.buf.borrow_mut();

        // Here we loop over the two TCP halves, reading all data from one
        // connection and writing it to another. The crucial performance aspect
        // of this server, however, is that we wait until both the read half and
        // the write half are ready on the connection, allowing the buffer to
        // only be temporarily used in a small window for all connections.
        loop {
            let read_ready = self.reader.poll_read().is_ready();
            let write_ready = self.writer.poll_write().is_ready();
            if !read_ready || !write_ready {
                return Ok(Async::NotReady);
            }

            // TODO: This exact logic for reading/writing amounts may need an
            //       update
            //
            // Right now the `buffer` is actually pretty big, 64k, and it could
            // be the case that one end of the connection can far outpace
            // another. For example we may be able to always read 64k from the
            // read half but only be able to write 5k to the client. This is a
            // pretty bad situation because we've got data in a buffer that's
            // intended to be ephemeral!
            //
            // Ideally here we'd actually adapt the rate of reads to match the
            // rate of writes. That is, we'd prefer to have some form of
            // adaptive algorithm which keeps track of how many bytes are
            // written and match the read rate to the write rate. It's possible
            // for connections to have an even smaller (and optional) buffer on
            // the side representing the "too much data they read" if that
            // happens, and then the next call to `read` could compensate by not
            // reading so much again.
            //
            // In any case, though, this is easily implementable in terms of
            // adding fields to `Transfer` and is complicated enough to
            // otherwise detract from the example in question here. As a result,
            // we simply read into the global buffer and then assert that we
            // write out exactly the same amount.
            //
            // This means that we may trip the assert below, but it should be
            // relatively easily fixable with the strategy above!

            let n = try_nb!((&*self.reader).read(&mut buffer));
            if n == 0 {
                try!(self.writer.shutdown(Shutdown::Write));
                return Ok(self.amt.into());
            }
            self.amt += n as u64;

            // Unlike above, we don't handle `WouldBlock` specially, because
            // that would play into the logic mentioned above (tracking read
            // rates and write rates), so we just ferry along that error for
            // now.
            let m = try!((&*self.writer).write(&buffer[..n]));
            assert_eq!(n, m);
        }
    }
}