use super::super::Path;
use super::super::connection::{Connection as _Connection, ctx};
use super::super::connector;
use futures::{Future, Poll};
use std::{cmp, io, net};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tacho;
use tokio_timer::Timer;

pub type Connection = _Connection<Ctx>;

const BASE_BACKOFF_MS: u64 = 500;
const MAX_BACKOFF_MS: u64 = 60 * 60 * 15; // 15 minutes

pub fn new(dst_name: Path, peer_addr: net::SocketAddr, weight: f32) -> Endpoint {
    Endpoint {
        dst_name,
        peer_addr,
        weight,
        state: Rc::new(RefCell::new(State::default())),
    }
}

/// Represents a single concrete traffic destination
pub struct Endpoint {
    dst_name: Path,
    peer_addr: net::SocketAddr,
    weight: f32,
    state: Rc<RefCell<State>>,
}

#[derive(Default)]
struct State {
    pending_conns: usize,
    open_conns: usize,
    consecutive_failures: usize,
    rx_bytes: usize,
    tx_bytes: usize,
}
impl State {
    fn backoff(&self) -> Option<Duration> {
        if self.consecutive_failures == 0 {
            None
        } else {
            let bo = BASE_BACKOFF_MS * self.consecutive_failures as u64;
            Some(Duration::from_millis(cmp::min(bo, MAX_BACKOFF_MS)))
        }
    }

    pub fn is_idle(&self) -> bool {
        self.open_conns == 0
    }
}

impl Endpoint {
    pub fn peer_addr(&self) -> net::SocketAddr {
        self.peer_addr
    }

    // TODO should we account for available connections?
    // TODO we should be able to use throughput/bandwidth as well.
    pub fn load(&self) -> usize {
        let s = self.state.borrow();
        s.open_conns + s.pending_conns + s.consecutive_failures.pow(2)
    }

    pub fn consecutive_failures(&self) -> usize {
        self.state.borrow().consecutive_failures
    }

    pub fn set_weight(&mut self, w: f32) {
        assert!(0.0 <= w && w <= 1.0);
        self.weight = w;
    }

    pub fn weighted_load(&self) -> usize {
        let load = (self.load() + 1) as f32;
        let factor = 100.0 * (1.0 - self.weight);
        (load * factor) as usize
    }

    pub fn connect(&self,
                   sock: connector::Connecting,
                   duration: &tacho::Timer,
                   timer: &Timer)
                   -> Connecting {
        let mut state = self.state.borrow_mut();
        state.pending_conns += 1;
        let conn = {
            let dst_name = self.dst_name.clone();
            let state = self.state.clone();
            let duration = duration.clone();
            sock.map(move |sock| {
                         let ctx = Ctx {
                             state,
                             duration,
                             start: Instant::now(),
                         };
                         Connection::new(dst_name, sock, ctx)
                     })
        };
        if let Some(backoff) = state.backoff() {
            debug!("{}: delaying new connection (#{})",
                   self.peer_addr,
                   state.consecutive_failures);
            let conn = timer
                .sleep(backoff)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                .and_then(move |_| conn);
            return Connecting(Box::new(conn));
        }

        Connecting(Box::new(conn))
    }

    pub fn is_idle(&self) -> bool {
        self.state.borrow().is_idle()
    }
}

pub struct Connecting(Box<Future<Item = Connection, Error = io::Error> + 'static>);
impl Future for Connecting {
    type Item = Connection;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Connection, io::Error> {
        self.0.poll()
    }
}

pub struct Ctx {
    state: Rc<RefCell<State>>,
    duration: tacho::Timer,
    start: Instant,
    // metrics: EndpointMetrics,
}

impl ctx::Ctx for Ctx {
    fn read(&mut self, sz: usize) {
        let mut state = self.state.borrow_mut();
        state.rx_bytes += sz;
    }

    fn wrote(&mut self, sz: usize) {
        let mut state = self.state.borrow_mut();
        state.tx_bytes += sz;
    }

    fn complete(self, res: &io::Result<()>) {
        let mut state = self.state.borrow_mut();
        state.open_conns -= 1;
        if res.is_ok() {
            state.consecutive_failures = 0;
        } else {
            state.consecutive_failures += 1;
        }
        self.duration.record_since(self.start)
    }
}
