//! A simple layer-4 load balancing library on tokio.
//!
//! Inspired by https://github.com/tailhook/tk-pool.
//!
//! TODO: if removed endpoints can't be considered for load balancing, they should be
//! removed from `endpoints.

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
/// May only be accessed from a single thread.  Use `Balancer::into_shared` for a
/// cloneable/shareable variant.
///
/// ## Panics
///
/// Panics if the `Stream` of address of resolutions ends. It must never complete.
///
pub struct Balancer<A> {
    // Streams address updates (i.e. from service discovery).
    addr: A,

    // The states of all known possibly-available endpoints.
    endpoints: VecDeque<Endpoint>,

    // Holds transfer data between socks (because we don't yet employ a 0-copy strategy).
    // This buffer is used for _all_ transfers in this balancer.
    buffer: Rc<RefCell<Vec<u8>>>,

    // Establishes downstream connections.
    handle: Handle,
}

impl<A> Balancer<A>
    where A: Stream<Item = Vec<WeightedAddr>, Error = io::Error>
{
    /// Creates a new balancer with the given address stream
    pub fn new(addr: A, buf: Rc<RefCell<Vec<u8>>>, handle: Handle) -> Balancer<A> {
        Balancer {
            addr: addr,
            buffer: buf,
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
                let new = addr_weight_map(&new);

                // Then, iterate through the existing endpoints, figuring out which ones
                // have been removed from service discovery, and updating the weights of
                // others.
                let same = update_endpoints(&mut self.endpoints, &new);

                // Finally, go through the new addresses again and add new nodes where
                // appropriate.
                add_new_endpoints(&mut self.endpoints, &new, &same);

                Ok(())
            }
        }
    }

    /// Checks all endpoints
    fn poll_endpoints(&mut self) -> io::Result<()> {
        trace!("polling {} endpoints", self.endpoints.len());
        for _ in 0..self.endpoints.len() {
            let mut ep = self.endpoints.pop_front().unwrap();
            ep.poll_connections()?;
            ep.start_connecting(&self.handle);
            if ep.removed && ep.active.is_empty() {
                debug!("endpoint evicted: {}", ep.addr);
            } else {
                self.endpoints.push_back(ep);
            }
        }
        Ok(())
    }

    /// Dispatches an `Upstream` to a downstream `Endpoint`, if possible.
    ///
    /// Chooses two endpoints at random and uses the lesser-loaded of the two.
    fn dispatch(&mut self, up: Upstream) -> StartSend<Upstream, io::Error> {
        trace!("choosing a downstream for {}", up.1);
        // Choose an endpoint.
        match self.endpoints.len() {
            // No endpoints yet. Hold off on proxying.
            0 => Ok(AsyncSink::NotReady(up)),
            1 => {
                // One endpoint, use it.
                let mut ep = &mut self.endpoints[0];
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
                    let ep0 = &self.endpoints[i0];
                    let ep1 = &self.endpoints[i1];
                    if ep0.load() <= ep1.load() {
                        trace!("downstream: {} *{} (not {} *{})",
                               ep0.addr,
                               ep0.weight,
                               ep1.addr,
                               ep1.weight);
                        i0
                    } else {
                        trace!("downstream: {} *{} (not {} *{})",
                               ep1.addr,
                               ep1.weight,
                               ep1.addr,
                               ep0.weight);
                        i1
                    }
                };
                // Once we know the index of the endpoint we want to use, we obtain a
                // mutable reference to begin proxying.
                let mut ep = &mut self.endpoints[i];

                // Give the upstream to the downstream endpoint.
                Ok(ep.transmit(up, self.buffer.clone()))
            }
        }
    }
}

fn addr_weight_map(new: &[WeightedAddr]) -> HashMap<SocketAddr, f32> {
    let mut s = HashMap::new();
    for wa in new {
        s.insert(wa.0, wa.1);
    }
    s
}

fn update_endpoints(eps: &mut VecDeque<Endpoint>,
                    new: &HashMap<SocketAddr, f32>)
                    -> HashSet<SocketAddr> {
    let mut same = HashSet::new();
    for ref mut ep in eps {
        match new.get(&ep.addr) {
            None => ep.removed = true,
            Some(&weight) => {
                ep.removed = false;
                ep.weight = weight;
                same.insert(ep.addr);
            }
        }
    }
    same
}

fn add_new_endpoints(eps: &mut VecDeque<Endpoint>,
                     new: &HashMap<SocketAddr, f32>,
                     same: &HashSet<SocketAddr>) {
    for (addr, weight) in new {
        if !same.contains(addr) {
            let ep = Endpoint::new(*addr, *weight);
            eps.push_back(ep);
        }
    }
}

/// Receives `Upstream` sockets to be dismatched to an underlying endpoint.
///
/// `start_send` returns `Async::Ready` if there is a downstream endpoint available, and
/// `Async::NotReady` otherwise.
///
/// `poll_complete` always returns `Async::NotReady`, since the load balancer may always
/// receive more upstreams.
impl<A> Sink for Balancer<A>
    where A: Stream<Item = Vec<WeightedAddr>, Error = io::Error>
{
    type SinkItem = Upstream;
    type SinkError = io::Error;

    /// Updates the list of endpoints before attempting to dispatch `upstream` to a
    /// downstream endpoint.
    fn start_send(&mut self,
                  upstream: Self::SinkItem)
                  -> StartSend<Self::SinkItem, Self::SinkError> {
        trace!("sending {}", upstream.1);
        self.poll_addr()?;
        self.poll_endpoints()?;
        self.dispatch(upstream)
    }

    /// Updates the list of endpoints as needed.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        trace!("polling");
        self.poll_addr()?;
        self.poll_endpoints()?;
        // Our work is never done...
        Ok(Async::NotReady)
    }
}

/// A single possibly-available load balancing endpoint.
struct Endpoint {
    addr: SocketAddr,
    weight: f32,

    // Indicates that the node has been removed from service discovery and may not be
    // available.  Nodes that are removed currently receive no new connections.
    removed: bool,

    /// Pending connection attempts.
    connecting: VecDeque<TcpStreamNew>,

    /// Connections that have been established but are not yet in
    /// active use.
    established: VecDeque<TcpStream>,

    /// Active TCP streams. The stream completed
    active: VecDeque<Duplex>,
}

impl Endpoint {
    pub fn new(a: SocketAddr, w: f32) -> Endpoint {
        Endpoint {
            addr: a,
            weight: w,
            removed: false,
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
                }
                Err(e) => {
                    trace!("{}: cannot establish connection: {}", self.addr, e);
                }
            }
        }
    }

    fn start_connecting(&mut self, handle: &Handle) {
        if !self.removed && self.established.is_empty() && self.connecting.is_empty() {
            trace!("connecting to {}", self.addr);
            self.connecting.push_back(TcpStream::connect(&self.addr, handle));
        }
    }

    // Checks the state of connections for this endpoint.
    //
    // When active streams have been completed, they are removed. When pending connections
    // have been established, they are stored to be dispatched.
    fn poll_connections(&mut self) -> io::Result<()> {
        trace!("{}: {} connections established",
               self.addr,
               self.established.len());
        self.poll_active();
        self.poll_connecting();
        Ok(())
    }

    /// Scores the endpoint.
    ///
    /// Uses the number of active connections, combined with the endpoint's weight, to
    /// produce a load score. A lightly-loaded, heavily-weighted endpoint will receive a
    /// score close to 0.0. An endpoint that cannot currently handle events produces a
    /// score of `f32::INFINITY`.
    ///
    /// TODO: Should this be extracted into a configurable strategy?
    fn load(&self) -> f32 {
        if self.removed || self.established.is_empty() {
            // If the endpoint is not ready to serve requests, it should not be
            // considered.
            f32::INFINITY
        } else {
            (1.0 + self.active.len() as f32) / self.weight
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
                let tx = Duplex::new(self.addr, down_stream, up_addr, up_stream, buf);
                self.active.push_front(tx);
                AsyncSink::Ready
            }
        }
    }
}