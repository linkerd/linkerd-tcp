use super::{Endpoints, EndpointMap, Waiter};
use super::endpoint::{self, Endpoint};
use super::super::Path;
use super::super::connection::Connection;
use super::super::connector::Connector;
use super::super::resolver::Resolve;
use futures::{Future, Stream, Sink, Poll, Async, AsyncSink, StartSend};
use rand::{self, Rng};
use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

pub fn new(reactor: Handle,
           timer: Timer,
           dst_name: Path,
           connector: Connector,
           resolve: Resolve,
           endpoints: Endpoints,
           metrics: &tacho::Scope)
           -> Dispatcher {
    Dispatcher {
        reactor,
        timer,
        dst_name,
        endpoints,
        resolve,
        max_waiters: connector.max_waiters(),
        min_connections: connector.min_connections(),
        fail_limit: connector.failure_limit(),
        fail_penalty: connector.failure_penalty(),
        connector,
        connecting: VecDeque::default(),
        connected: VecDeque::default(),
        waiters: VecDeque::default(),
        metrics: Metrics::new(metrics),
    }
}

/// Accepts connection requests
pub struct Dispatcher {
    reactor: Handle,
    timer: Timer,
    dst_name: Path,
    connector: Connector,
    resolve: Resolve,
    endpoints: Endpoints,
    connecting: VecDeque<tacho::Timed<endpoint::Connecting>>,
    min_connections: usize,
    connected: VecDeque<Connection<endpoint::Ctx>>,
    waiters: VecDeque<Waiter>,
    max_waiters: usize,
    fail_limit: usize,
    fail_penalty: Duration,
    metrics: Metrics,
}

impl Dispatcher {
    /// Selects an endpoint using the power of two choices.
    ///
    /// We select 2 endpoints randomly, compare their weighted loads
    fn select_endpoint(available: &EndpointMap) -> Option<&Endpoint> {
        match available.len() {
            0 => None,
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
                let (load0, load1) = (ep0.load(), ep1.load());
                let (weight0, weight1) = (ep0.weight(), ep1.weight());

                let v0 = (load0 + 1) as f64 * (1.0 - weight0);
                let v1 = (load1 + 1) as f64 * (1.0 - weight1);
                let sum = v0 + v1;
                let boundary = sum - v0 / sum;
                let roll = rng.next_f64();
                if roll <= boundary {
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
        debug!("dispatching {} connections to {} waiters",
               self.connected.len(),
               self.waiters.len());
        while let Some(conn) = self.connected.pop_front() {
            if let Err(conn) = self.dispatch_to_next_waiter(conn) {
                self.connected.push_front(conn);
                return;
            }
        }
    }

    fn connect(&mut self) {
        let available = self.endpoints.available();
        if available.is_empty() {
            trace!("no available endpoints");
            return;
        }

        let needed = {
            let needed = self.min_connections + self.waiters.len();
            let pending = self.connecting.len() + self.connected.len();
            if needed < pending {
                0
            } else {
                needed - pending
            }
        };
        debug!("initiating {} connections", needed);

        for _ in 0..needed {
            match Dispatcher::select_endpoint(available) {
                None => {
                    trace!("no endpoints ready");
                    self.metrics.unavailable.incr(1);
                    return;
                }
                Some(ep) => {
                    self.metrics.attempts.incr(1);
                    let mut conn = {
                        let sock = self.connector
                            .connect(&ep.peer_addr(), &self.reactor, &self.timer);
                        let c = ep.connect(sock, &self.metrics.connection_duration);
                        self.metrics.connect_latency.time(c)
                    };
                    match conn.poll() {
                        Err(e) => {
                            debug!("connection failed: {}", e);
                            self.metrics.failure(&e);
                        }
                        Ok(Async::NotReady) => {
                            trace!("connection pending");
                            self.metrics.pending.incr(1);
                            self.connecting.push_back(conn);
                        }
                        Ok(Async::Ready(conn)) => {
                            debug!("connected");
                            self.metrics.connects.incr(1);
                            self.metrics.pending.decr(1);
                            self.metrics.open.incr(1);
                            self.connected.push_back(conn);
                        }
                    }
                }
            }
        }
    }

    fn poll_resolve(&mut self) {
        loop {
            match self.resolve.poll() {
                Err(e) => {
                    error!("{}: resolver error: {:?}", self.dst_name, e);
                }
                Ok(Async::Ready(Some(Err(e)))) => {
                    error!("{}: resolver error: {:?}", self.dst_name, e);
                }
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(None)) => {
                    info!("resolution complete! no further updates will be received");
                    break;
                }
                Ok(Async::Ready(Some(Ok(addrs)))) => {
                    self.endpoints.update_resolved(&self.dst_name, &addrs);
                }
            }
        }
        debug!("balancer updated: available={} failed={}, retired={}",
               self.endpoints.available().len(),
               self.endpoints.failed().len(),
               self.endpoints.retired().len());
    }

    fn poll_connecting(&mut self) {
        debug!("polling {} pending connections", self.connecting.len());
        for _ in 0..self.connecting.len() {
            let mut conn = self.connecting.pop_front().unwrap();
            match conn.poll() {
                Err(e) => {
                    debug!("connection failed: {}", e);
                    self.metrics.pending.decr(1);
                    self.metrics.failure(&e);
                }
                Ok(Async::NotReady) => {
                    trace!("connection pending");
                    self.connecting.push_back(conn);
                }
                Ok(Async::Ready(conn)) => {
                    debug!("connected");
                    self.metrics.connects.incr(1);
                    self.metrics.pending.decr(1);
                    self.metrics.open.incr(1);
                    self.connected.push_back(conn)
                }
            }
        }
    }

    fn record(&self, t0: Instant) {
        {
            let mut open = 0;
            let mut pending = 0;
            {
                let available = self.endpoints.available();
                self.metrics.available.set(available.len());
                for ep in available.values() {
                    let state = ep.state();
                    open += state.open_conns;
                    pending += state.pending_conns;
                }
            }
            {
                let failed = self.endpoints.failed();
                self.metrics.failed.set(failed.len());
                for &(_, ref ep) in failed.values() {
                    let state = ep.state();
                    open += state.open_conns;
                    pending += state.pending_conns;
                }
            }
            {
                let retired = self.endpoints.retired();
                self.metrics.retired.set(retired.len());
                for ep in retired.values() {
                    let state = ep.state();
                    open += state.open_conns;
                    pending += state.pending_conns;
                }
            }
            self.metrics.open.set(open);
            self.metrics.pending.set(pending);
        }
        self.metrics.waiters.set(self.waiters.len());
        self.metrics.poll_time.record_since(t0);
    }

    fn assess_failure(&mut self) {
        self.endpoints
            .update_failed(self.fail_limit, self.fail_penalty);
    }

    fn poll(&mut self) {
        let t0 = Instant::now();
        self.poll_connecting();
        // We drive resolution from this task so that updates can trigger dispatch i.e. if
        // waiters are waiting for endpoints to be added.
        self.poll_resolve();
        self.connect();
        self.dispatch_connected_to_waiters();
        self.assess_failure();
        self.record(t0);
    }
}

/// Buffers up to `max_waiters` concurrent connection requests, along with corresponding connection attempts.
impl Sink for Dispatcher {
    type SinkItem = Waiter;
    type SinkError = io::Error;

    fn start_send(&mut self, waiter: Waiter) -> StartSend<Waiter, io::Error> {
        if self.waiters.len() == self.max_waiters {
            return Ok(AsyncSink::NotReady(waiter));
        }
        self.waiters.push_back(waiter);
        self.poll();
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        if self.connecting.is_empty() && self.waiters.is_empty() {
            return Ok(Async::Ready(()));
        }
        self.poll();
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
    unavailable: tacho::Counter,
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
            unavailable: root.counter("unavailable"),
            attempts: conn.counter("attempts"),
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
