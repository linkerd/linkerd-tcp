//! A simple layer-4 load balancing library on tokio.
//!
//! Inspired by https://github.com/tailhook/tk-pool.
//!
//! Copyright 2016 The tk-pool Developers
//! Copyright 2017 Buoyant, Inc.

use futures::{StartSend, AsyncSink, Async, Future, Poll, Sink, Stream};
use rand::{self, Rng};
use std::{f32, io};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::net::SocketAddr;
use tokio_core::reactor::Handle;
use tokio_core::net::{TcpStream, TcpStreamNew};

mod driver;
mod duplex;
mod proxy_stream;
mod shared;

use lb::driver::Driver;
use lb::duplex::Duplex;
use lb::proxy_stream::ProxyStream;
pub use lb::shared::Shared;

use WeightedAddr;

pub type Upstream = (TcpStream, SocketAddr);

/// Distributes TCP connections across a pool of downstreams.
///
/// May only be accessed from a single thread.
///
/// ## Panics ##
///
/// Panics when a peer is unable to receive an entire write (because we don't have any
/// provisions for maintaining a buffer between polls). This is a bug.
pub struct Balancer<A> {
    // Streams address updates (i.e. from service discovery).
    addr: A,

    // The state of
    endpoints: VecDeque<(SocketAddr, Endpoint)>,

    // A single buffer, used by all connections, to store data when transferring between remotes.
    buffer: Rc<RefCell<Vec<u8>>>,

    // Establishes downstream connections.
    handle: Handle,
}

impl<A> Balancer<A>
    where A: Stream<Item = Vec<WeightedAddr>, Error = io::Error>
{
    /// Creates a new balancer with the given address stream
    pub fn new(addr: A, bufsz: usize, handle: Handle) -> Balancer<A> {
        Balancer {
            addr: addr,
            buffer: Rc::new(RefCell::new(vec![0;bufsz])),
            endpoints: VecDeque::new(),
            handle: handle,
        }
    }

    /// Moves this balancer into one that may be shared across threads.
    ///
    /// The Balancer's handle is used to drive all balancer changes on a single thread,
    /// while other threads may submit `Upstreams` to be processed.
    pub fn into_shared(self, max_waiters: usize) -> Shared
        where A: 'static
    {
        let h = self.handle.clone();
        Shared::new(self, max_waiters, &h)
    }

    /// Checks if the addr has updated.  If it has, update
    /// `endpoints` new addresses and weights.
    fn poll_addr(&mut self) -> io::Result<()> {
        trace!("polling addr");
        // First, see if address addr has changed.  If it has,
        // update the load balancer's endpoints.
        match self.addr.poll() {
            Err(e) => Err(e),
            Ok(Async::NotReady) => Ok(()),
            Ok(Async::Ready(None)) => panic!("address addr stream must be infinite"),
            Ok(Async::Ready(Some(new))) => {
                trace!("addr update");

                // First, put all of the new addrs in a hash.
                let new = {
                    let mut s = HashMap::new();
                    for ref wa in new.iter() {
                        s.insert(wa.0.clone(), wa.clone());
                    }
                    s
                };

                // Then, iterate through the existing endpoints,
                // figuring out which ones have been removed from
                // service discovery, and updating the weights of others.
                let same = {
                    let mut same = HashSet::new();
                    for mut addr_ep in self.endpoints.iter_mut() {
                        let (ref addr, ref mut ep) = *addr_ep;
                        match new.get(addr) {
                            None => ep.removed = true,
                            Some(&&WeightedAddr(_, weight)) => {
                                ep.removed = false;
                                ep.weight = weight.clone();
                                same.insert(addr.clone());
                            }
                        }
                    }
                    same
                };

                // Finally, go through the new addresses again and add
                // new nodes where appropriate.
                for (addr, weight) in new.iter() {
                    let addr = addr.clone();
                    if !same.contains(&addr) {
                        let ep = Endpoint::new((*weight).clone());
                        self.endpoints.push_back((addr, ep));
                    }
                }

                Ok(())
            }
        }
    }

    /// Checks all endpoints
    fn poll_endpoints(&mut self) -> io::Result<()> {
        trace!("polling {} endpoints", self.endpoints.len());
        for _ in 0..self.endpoints.len() {
            let (addr, mut ep) = self.endpoints.pop_front().unwrap();
            ep.poll_connections()?;
            ep.start_connecting(&self.handle);
            if ep.removed && ep.active.len() == 0 {
                debug!("endpoint evicted: {}", addr);
            } else {
                self.endpoints.push_back((addr, ep));
            }
        }
        Ok(())
    }

    /// Begins proxying between the upstream and a downstream endpoint.
    ///
    /// Chooses two endpoints at random and uses the lesser-loaded of the two.
    fn start_proxying(&mut self, up: Upstream) -> StartSend<Upstream, io::Error> {
        trace!("choosing a downstream for {}", up.1);
        // Choose an endpoint.
        match self.endpoints.len() {
            // No endpoints yet. Hold off on proxying.
            0 => Ok(AsyncSink::NotReady(up)),
            1 => {
                // One endpoint, use it.
                let ref mut eps = self.endpoints;
                let (_, ref mut ep) = eps[0];
                Ok(ep.transmit(up, self.buffer.clone()))
            }
            sz => {
                // Determine candidate indices.
                let (i0, i1) = if sz == 2 {
                    // There are only two endpoints, so no need for an RNG.
                    (0, 1)
                } else {
                    // 3 or more endpoints: choose two distinct endpoints at random.
                    let mut rng = rand::thread_rng();
                    let i0 = rng.gen_range(0, sz);
                    let mut i1 = rng.gen_range(0, sz);
                    while i0 == i1 {
                        i1 = rng.gen_range(0, sz);
                    }
                    (i0, i1)
                };
                // Determine the index of the lesser-loaded endpoint
                let i = {
                    let (a0, ref ep0) = self.endpoints[i0];
                    let (a1, ref ep1) = self.endpoints[i1];
                    if ep0.load() <= ep1.load() {
                        trace!("downstream: {} *{} (not {} *{})",
                               a0,
                               ep0.weight,
                               a1,
                               ep1.weight);
                        i0
                    } else {
                        trace!("downstream: {} *{} (not {} *{})",
                               a1,
                               ep1.weight,
                               a0,
                               ep0.weight);
                        i1
                    }
                };
                // Once we know the index of the endpoint we want to use, we obtain a
                // mutable reference to begin proxying.
                let (_, ref mut ep) = *self.endpoints.get_mut(i).unwrap();

                // Give the upstream to the downstream endpoint.
                Ok(ep.transmit(up, self.buffer.clone()))
            }
        }
    }
}

/// Receives `Upstream` sockets
impl<A> Sink for Balancer<A>
    where A: Stream<Item = Vec<WeightedAddr>, Error = io::Error>
{
    type SinkItem = Upstream;
    type SinkError = io::Error;

    fn start_send(&mut self,
                  upstream: Self::SinkItem)
                  -> StartSend<Self::SinkItem, Self::SinkError> {
        trace!("sending {}", upstream.1);
        self.poll_addr()?;
        self.poll_endpoints()?;
        self.start_proxying(upstream)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        trace!("polling");
        self.poll_addr()?;
        self.poll_endpoints()?;
        // Our work is never done...
        Ok(Async::NotReady)
    }
}

/// A single endpoint
struct Endpoint {
    addr: SocketAddr,
    weight: f32,

    // Indicates that the node has been removed from service discovery
    // and may not be available.
    removed: bool,

    // The number of consecutive failures observed on this endpoint.
    consecutive_failures: usize,

    /// Pending connections.
    connecting: VecDeque<TcpStreamNew>,

    /// Connections that have been established but are not yet in
    /// active use.
    established: VecDeque<TcpStream>,

    /// Active TCP streams. The stream completed
    active: VecDeque<Duplex>,
}

impl Endpoint {
    pub fn new(wa: WeightedAddr) -> Endpoint {
        Endpoint {
            addr: wa.0,
            weight: wa.1,
            removed: false,
            consecutive_failures: 0,
            connecting: VecDeque::new(),
            established: VecDeque::new(),
            active: VecDeque::new(),
        }
    }

    fn poll_active(&mut self) {
        let n = self.active.len();
        for i in 0..n {
            let mut active = self.active.pop_front().unwrap();
            trace!("{}: polling active stream {}/{}", self.addr, i + 1, n);
            match active.poll() {
                Ok(Async::NotReady) => {
                    self.active.push_back(active);
                }
                Ok(Async::Ready((down_bytes, up_bytes))) => {
                    trace!("{}: completed from {}: {}B down / {}B up",
                           self.addr,
                           active.up_addr,
                           down_bytes,
                           up_bytes);
                }
                Err(e) => {
                    info!("{}: failed from {}: {}", self.addr, active.up_addr, e);
                    self.consecutive_failures += 1;
                }
            }
        }
    }

    fn poll_connecting(&mut self) {
        let n = self.connecting.len();
        trace!("{}: polling {} pending streams", self.addr, n);
        for _ in 0..n {
            let mut c = self.connecting.pop_front().unwrap();
            match c.poll() {
                Ok(Async::NotReady) => {
                    self.connecting.push_back(c);
                }
                Ok(Async::Ready(c)) => {
                    trace!("{}: connection established", self.addr);
                    self.established.push_back(c);
                    self.consecutive_failures = 0;
                }
                Err(e) => {
                    trace!("{}: cannot establish connection: {}", self.addr, e);
                    self.consecutive_failures += 1;
                }
            }
        }
    }

    fn start_connecting(&mut self, handle: &Handle) {
        if !self.removed && (self.established.len() + self.connecting.len() == 0) {
            trace!("connecting to {}", self.addr);
            self.connecting.push_back(TcpStream::connect(&self.addr, handle));
        }
    }

    fn poll_connections(&mut self) -> io::Result<()> {
        trace!("{}: {} connections established",
               self.addr,
               self.established.len());
        self.poll_active();
        self.poll_connecting();
        Ok(())
    }

    /// Score the endpoint.  Lower scores are healthier.
    ///
    /// ## Todo ##
    ///
    /// This should be extracted into a configurable strategy, effectively implmenting
    /// Fn(&Endpoint) -> f32.
    fn load(&self) -> f32 {
        if self.removed {
            f32::INFINITY
        } else {
            let mut weight = self.weight;
            if self.consecutive_failures > 0 {
                weight /= 2u32.pow(self.consecutive_failures as u32) as f32;
            } else if self.established.len() > 0 {
                weight *= 1.2;
            } else if self.connecting.len() > 0 {
                weight *= 1.05;
            }
            self.active.len() as f32 / weight
        }
    }

    /// Attempts to begin transmiting from the the `Upstream` to this endpoint.
    ///
    /// If no connections have been established, the Upstrea is returned in an
    /// `Async::NotReady` so that the caller may try another endpoint.
    fn transmit(&mut self, up: Upstream, buf: Rc<RefCell<Vec<u8>>>) -> AsyncSink<Upstream> {
        match self.established.pop_front() {
            None => {
                trace!("no connections to {} from {}", up.1, self.addr);
                AsyncSink::NotReady(up)
            }
            Some(down_stream) => {
                let (up_stream, up_addr) = up;
                trace!("transmitting to {} from {}", self.addr, up_addr);
                let tx = duplex::new(self.addr.clone(), down_stream, up_addr, up_stream, buf);
                self.active.push_front(tx);
                AsyncSink::Ready
            }
        }
    }
}