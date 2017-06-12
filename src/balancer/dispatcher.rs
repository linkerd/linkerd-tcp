use super::{Endpoints, EndpointMap, Waiter};
use super::endpoint::{self, Endpoint};
use super::super::connection::Connection;
use super::super::connector::Connector;
use futures::{Future, Sink, Poll, Async, AsyncSink, StartSend};
use rand::{self, Rng};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use std::time::Instant;
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

pub fn new(reactor: Handle,
           timer: Timer,
           connector: Connector,
           endpoints: Rc<RefCell<Endpoints>>,
           min_connections: usize,
           max_waiters: usize,
           metrics: &tacho::Scope)
           -> Dispatcher {
    Dispatcher {
        reactor,
        timer,
        connector,
        endpoints,
        min_connections,
        max_waiters,
        connecting: VecDeque::with_capacity(max_waiters),
        connected: VecDeque::with_capacity(max_waiters),
        waiters: VecDeque::with_capacity(max_waiters),
        metrics: Metrics::new(metrics),
    }
}

/// Accepts connection requests
pub struct Dispatcher {
    reactor: Handle,
    timer: Timer,
    connector: Connector,
    endpoints: Rc<RefCell<Endpoints>>,
    connecting: VecDeque<tacho::Timed<endpoint::Connecting>>,
    min_connections: usize,
    connected: VecDeque<Connection<endpoint::Ctx>>,
    waiters: VecDeque<Waiter>,
    max_waiters: usize,
    metrics: Metrics,
}

impl Dispatcher {
    /// Selects an endpoint using the power of two choices.
    ///
    /// We select 2 endpoints randomly, compare their weighted loads
    fn select_endpoint(available: &EndpointMap) -> Option<&Endpoint> {
        match available.len() {
            0 => {
                trace!("no endpoints ready");
                None
            }
            1 => {
                // One endpoint, use it.
                available.get_index(0).map(|(_, ep)| ep)
            }
            sz => {
                let mut rng = rand::thread_rng();

                // Pick 2 candidate indices.
                let (i0, i1) = if sz == 2 {
                    // There are only two endpoints, so no need for an RNG.
                    (0, 1)
                } else {
                    // 3 or more endpoints: choose two distinct endpoints at random.
                    let i0 = rng.gen_range(0, sz);
                    let mut i1 = rng.gen_range(0, sz);
                    while i0 == i1 {
                        i1 = rng.gen_range(0, sz);
                    }
                    (i0, i1)
                };

                // Determine the index of the lesser-loaded endpoint
                let (addr0, ep0) = available.get_index(i0).unwrap();
                let (addr1, ep1) = available.get_index(i1).unwrap();
                let (load0, load1) = (ep0.weighted_load(), ep1.weighted_load());

                let use0 = if load0 == load1 {
                    rng.gen::<bool>()
                } else {
                    load0 < load1
                };

                if use0 {
                    trace!("dst: {} {} (not {} {})", addr0, load0, addr1, load1);
                    Some(ep0)
                } else {
                    trace!("dst: {} {} (not {} {})", addr1, load1, addr0, load0);
                    Some(ep1)
                }
            }
        }
    }

    fn dispatch_to_next_waiter(&mut self,
                               conn: endpoint::Connection)
                               -> Result<(), endpoint::Connection> {
        match self.waiters.pop_front() {
            None => Err(conn),
            Some(waiter) => {
                match waiter.send(conn) {
                    Ok(()) => Ok(()),
                    Err(conn) => self.dispatch_to_next_waiter(conn),
                }
            }
        }
    }

    fn dispatch_connected_to_waiters(&mut self) {
        while let Some(conn) = self.connected.pop_front() {
            if let Err(conn) = self.dispatch_to_next_waiter(conn) {
                self.connected.push_front(conn);
                return;
            }
        }
    }

    fn connect(&mut self) -> io::Result<usize> {
        let endpoints = self.endpoints.borrow();
        let available = endpoints.available();
        let mut connected = 0;
        let needed = self.min_connections +
                     (self.waiters.len() - self.connecting.len() + self.connected.len());
        for _ in 0..needed {
            match Dispatcher::select_endpoint(available) {
                None => {
                    return Ok(connected);
                }
                Some(ep) => {
                    let sock = self.connector
                        .connect(&ep.peer_addr(), &self.reactor, &self.timer);
                    let mut conn = self.metrics
                        .connect_latency
                        .time(ep.connect(sock, &self.timer));
                    self.metrics.attempts.incr(1);
                    match conn.poll() {
                        Err(e) => {
                            self.metrics.failure(&e);
                            return Err(e);
                        }
                        Ok(Async::NotReady) => {
                            self.metrics.pending.incr(1);
                            self.connecting.push_back(conn)
                        }
                        Ok(Async::Ready(conn)) => self.connected.push_back(conn),
                    }
                    connected += 1;
                }
            }
        }
        Ok(connected)
    }

    fn poll_connecting(&mut self) -> io::Result<()> {
        for _ in 0..self.connecting.len() {
            let mut conn = self.connecting.pop_front().unwrap();
            match conn.poll() {
                Err(e) => {
                    self.metrics.pending.decr(1);
                    self.metrics.failure(&e);
                    return Err(e);
                }
                Ok(Async::NotReady) => self.connecting.push_back(conn),
                Ok(Async::Ready(conn)) => {
                    self.metrics.pending.decr(1);
                    self.metrics.open.incr(1);
                    self.metrics.connects.incr(1);
                    self.connected.push_back(conn)
                }
            }
        }
        Ok(())
    }

    fn record(&self, t0: Instant) {
        let endpoints = self.endpoints.borrow();
        {
            let available = endpoints.available();
            self.metrics.available.set(available.len());
            let failed = available
                .values()
                .filter(|ep| ep.consecutive_failures() > 0)
                .count();
            self.metrics.failed.set(failed)
        }
        self.metrics.retired.set(endpoints.retired.len());
        self.metrics.poll_time.record_since(t0);
        self.metrics.waiters.set(self.waiters.len());
    }

    fn poll(&mut self) -> io::Result<()> {
        let t0 = Instant::now();
        self.poll_connecting()?;
        self.connect()?;
        self.dispatch_connected_to_waiters();
        self.record(t0);
        Ok(())
    }
}

/// Buffers up to `max_waiters` concurrent connection requests, along with corresponding connection attempts.
impl Sink for Dispatcher {
    type SinkItem = Waiter;
    type SinkError = io::Error;

    fn start_send(&mut self, waiter: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if self.waiters.len() == self.max_waiters {
            return Ok(AsyncSink::NotReady(waiter));
        }
        self.waiters.push_back(waiter);
        self.poll()?;
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if self.connecting.is_empty() && self.waiters.is_empty() {
            return Ok(Async::Ready(()));
        }
        self.poll()?;;
        Ok(Async::NotReady)
    }
}

struct Metrics {
    available: tacho::Gauge,
    failed: tacho::Gauge,
    retired: tacho::Gauge,
    pending: tacho::Gauge,
    open: tacho::Gauge,
    waiters: tacho::Gauge,
    poll_time: tacho::Timer,
    attempts: tacho::Counter,
    connects: tacho::Counter,
    timeouts: tacho::Counter,
    refused: tacho::Counter,
    failures: tacho::Counter,
    connect_latency: tacho::Timer,
    connection_duration: tacho::Timer,
}

impl Metrics {
    fn new(root: &tacho::Scope) -> Metrics {
        let ep = root.clone().prefixed("endpoint");
        let conn = root.clone().prefixed("connection");
        Metrics {
            available: ep.gauge("available"),
            failed: ep.gauge("failed"),
            retired: ep.gauge("retired"),
            pending: conn.gauge("pending"),
            open: conn.gauge("open"),
            waiters: root.gauge("waiters"),
            poll_time: root.timer_us("poll_time_us"),
            attempts: conn.counter("aggempts"),
            connects: conn.counter("connects"),
            timeouts: conn.clone().labeled("cause", "timeout").counter("failure"),
            refused: conn.clone().labeled("cause", "refused").counter("failure"),
            failures: conn.clone().labeled("cause", "other").counter("failure"),
            connect_latency: conn.timer_us("latency_us"),
            connection_duration: conn.timer_us("duration_ms"),
        }
    }

    fn failure(&self, err: &io::Error) {
        match err.kind() {
            io::ErrorKind::TimedOut => self.timeouts.incr(1),
            io::ErrorKind::ConnectionRefused => self.refused.incr(1),
            _ => self.failures.incr(1),
        }
    }
}
