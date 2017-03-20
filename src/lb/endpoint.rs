use futures::{Async, AsyncSink, Future};
use std::{f32, io};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::net::SocketAddr;

use lb::{Connector, Duplex, Src, Dst, WithAddr};

/// A single possibly-available load balancing endpoint.
pub struct Endpoint {
    addr: SocketAddr,
    weight: f32,
    retired: bool,

    /// Pending connection attempts.
    pending: VecDeque<Box<Future<Item = Dst, Error = io::Error>>>,

    /// Connections that have been established but are not yet in
    /// active use.
    established: VecDeque<Dst>,

    /// Active TCP streams. The stream completed
    active: VecDeque<Duplex>,
}

impl Endpoint {
    pub fn new(a: SocketAddr, w: f32) -> Endpoint {
        Endpoint {
            addr: a,
            weight: w,
            retired: false,
            pending: VecDeque::new(),
            established: VecDeque::new(),
            active: VecDeque::new(),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn weight(&self) -> f32 {
        self.weight
    }

    pub fn set_weight(&mut self, w: f32) {
        self.weight = w;
    }

    pub fn is_retired(&self) -> bool {
        self.retired
    }

    pub fn retire(&mut self) {
        for _ in 0..self.pending.len() {
            drop(self.pending.pop_front());
        }
        for _ in 0..self.established.len() {
            drop(self.established.pop_front());
        }
        self.retired = true;
    }

    pub fn unretire(&mut self) {
        self.retired = false;
    }

    pub fn conns_pending(&self) -> usize {
        self.pending.len()
    }

    pub fn conns_established(&self) -> usize {
        self.established.len()
    }

    pub fn conns_active(&self) -> usize {
        self.active.len()
    }

    pub fn is_ready(&self) -> bool {
        !self.established.is_empty()
    }

    pub fn is_active(&self) -> bool {
        !self.active.is_empty()
    }

    /// Scores the endpoint.
    ///
    /// Uses the number of active connections, combined with the endpoint's weight, to
    /// produce a load score. A lightly-loaded, heavily-weighted endpoint will receive a
    /// score close to 0.0. An endpoint that cannot currently handle events produces a
    /// score of `f32::INFINITY`.
    ///
    /// TODO: Should this be extracted into a configurable strategy?
    pub fn load(&self) -> f32 {
        if !self.is_ready() {
            // If the endpoint is not ready to serve requests, it should not be
            // considered.
            f32::INFINITY
        } else {
            (1.0 + self.conns_active() as f32) / self.weight
        }
    }

    /// Initiate a new connection
    pub fn init_connection<C: Connector>(&mut self, c: &C) {
        debug!("initiating connection to {}", self.addr);
        let f = c.connect(&self.addr);
        self.pending.push_back(f);
    }

    /// Checks the state of connections for this endpoint.
    ///
    /// When active streams have been completed, they are removed. When pending
    /// connections have been established, they are stored to be dispatched.
    pub fn poll_connections(&mut self) -> io::Result<()> {
        trace!("{}: {} connections established",
               self.addr,
               self.established.len());
        self.poll_active();
        self.poll_pending();
        Ok(())
    }

    fn poll_active(&mut self) {
        let sz = self.active.len();
        trace!("{}: checking {} active streams", self.addr, sz);
        for i in 0..sz {
            let mut active = self.active.pop_front().unwrap();
            trace!("{}: polling active stream {}/{}: {}",
                   self.addr,
                   i + 1,
                   sz,
                   active.src_addr);
            match active.poll() {
                Ok(Async::NotReady) => {
                    trace!("{}: still active from {}", self.addr, active.src_addr);
                    self.active.push_back(active);
                }
                Ok(Async::Ready((dst_bytes, src_bytes))) => {
                    trace!("{}: completed from {}: {}B dst / {}B src",
                           self.addr,
                           active.src_addr,
                           dst_bytes,
                           src_bytes);
                    drop(active);
                }
                Err(e) => {
                    info!("{}: failed from {}: {}", self.addr, active.src_addr, e);
                    drop(active);
                }
            }
        }
    }

    fn poll_pending(&mut self) {
        let sz = self.pending.len();
        trace!("{}: polling {} pending streams", self.addr, sz);
        for _ in 0..sz {
            let mut c = self.pending.pop_front().unwrap();
            match c.poll() {
                Ok(Async::NotReady) => {
                    self.pending.push_back(c);
                }
                Ok(Async::Ready(c)) => {
                    trace!("{}: connection established", self.addr);
                    self.established.push_back(c);
                }
                Err(e) => {
                    info!("{}: cannot establish connection: {}", self.addr, e);
                }
            }
        }
    }


    /// Attempts to begin transmiting from the the `Src` to this endpoint.
    ///
    /// If no connections have been established, the Upstrea is returned in an
    /// `Async::NotReady` so that the caller may try another endpoint.
    pub fn transmit(&mut self, src: Src, buf: Rc<RefCell<Vec<u8>>>) -> AsyncSink<Src> {
        match self.established.pop_front() {
            None => {
                {
                    let Src(ref src) = src;
                    trace!("no connections to {} from {}", self.addr, src.addr());
                }
                AsyncSink::NotReady(src)
            }
            Some(Dst(dst)) => {
                let Src(src) = src;
                trace!("transmitting to {} from {}", self.addr, src.addr());
                let tx = Duplex::new(src, dst, buf);
                self.active.push_front(tx);
                AsyncSink::Ready
            }
        }
    }
}