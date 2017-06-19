use super::{Endpoints, EndpointMap, Waiter, WeightedAddr};
use super::endpoint::{self, Endpoint};
use super::super::Path;
use super::super::connection::Connection;
use super::super::connector::Connector;
use super::super::resolver::Resolve;
use futures::{Future, Stream, Poll, Async};
use rand::{self, Rng};
use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

pub fn new<S>(
    reactor: Handle,
    timer: Timer,
    dst_name: Path,
    connector: Connector,
    resolve: Resolve,
    waiters_rx: S,
    endpoints: Endpoints,
    metrics: &tacho::Scope,
) -> Dispatcher<S>
where
    S: Stream<Item = Waiter>,
{
    Dispatcher {
        reactor,
        timer,
        dst_name,
        endpoints,
        resolve,
        waiters_rx,
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

/// Initiates load balanced outbound connections.
pub struct Dispatcher<W> {
    reactor: Handle,
    timer: Timer,

    /// Names the destination replica set to which connections are being dispatched.
    dst_name: Path,

    /// Handles destination-specific connection policy.
    connector: Connector,

    /// Provides new service discovery resolutions as a Stream.
    resolve: Resolve,

    /// Holds the state of all available/failed/retired endpoints.
    endpoints: Endpoints,

    /// Limits the number of consecutive failures allowed before an endpoint is marked as
    /// failed.
    fail_limit: usize,

    /// Controls how long an endpoint will be marked as failed before being considered for
    /// new connections.s
    fail_penalty: Duration,

    /// Controls the minimum number of connecting/connected connections to be maintained
    /// at all times.
    min_connections: usize,

    /// A queue of pending connections.
    connecting: VecDeque<tacho::Timed<endpoint::Connecting>>,

    /// A queue of ready connections to be dispatched ot waiters.
    connected: VecDeque<Connection<endpoint::Ctx>>,

    /// Provides new connection requests as a Stream..
    waiters_rx: W,

    /// A queue of waiters that have not yet received a connection.
    waiters: VecDeque<Waiter>,

    /// Limits the size of `waiters`.
    max_waiters: usize,

    metrics: Metrics,
}

impl<W> Dispatcher<W>
where
    W: Stream<Item = Waiter>,
{
    /// Receives and attempts to dispatch new waiters.
    ///
    /// If there are no available connections to be dispatched, up to `max_waiters` are
    /// buffered.
    fn recv_waiters(&mut self) {
        while self.waiters.len() < self.max_waiters {
            match self.waiters_rx.poll() {
                Ok(Async::Ready(None)) |
                Ok(Async::NotReady) => return,
                Err(_) => {
                    error!("{}: error from waiters channel", self.dst_name);
                }
                Ok(Async::Ready(Some(w))) => {
                    match self.connected.pop_front() {
                        None => self.waiters.push_back(w),
                        Some(conn) => {
                            if let Err(conn) = w.send(conn) {
                                self.connected.push_front(conn);
                            }
                        }
                    }
                }
            }
        }
    }

    fn poll_connecting(&mut self) {
        debug!("polling {} pending connections", self.connecting.len());
        for _ in 0..self.connecting.len() {
            let mut connecting = self.connecting.pop_front().unwrap();
            match connecting.poll() {
                Err(e) => {
                    debug!("connection failed: {}", e);
                    self.metrics.pending.decr(1);
                    self.metrics.failure(&e);
                }
                Ok(Async::NotReady) => {
                    trace!("connection pending");
                    self.connecting.push_back(connecting);
                }
                Ok(Async::Ready(connected)) => {
                    debug!("connected");
                    self.metrics.connects.incr(1);
                    self.metrics.pending.decr(1);
                    self.metrics.open.incr(1);
                    self.connected.push_back(connected)
                }
            }
        }
    }

    fn update_endpoints(&mut self) {
        if let Some(addrs) = self.poll_resolve() {
            self.endpoints.update_resolved(&addrs);
            debug!(
                "balancer updated: available={} failed={}, retired={}",
                self.endpoints.available().len(),
                self.endpoints.failed().len(),
                self.endpoints.retired().len()
            );
        }

        self.endpoints.update_failed(
            self.fail_limit,
            self.fail_penalty,
        );

    }

    fn poll_resolve(&mut self) -> Option<Vec<WeightedAddr>> {
        // Poll the resolution until it's
        let mut addrs = None;
        loop {
            match self.resolve.poll() {
                Ok(Async::NotReady) => {
                    return addrs;
                }
                Ok(Async::Ready(None)) => {
                    info!("resolution complete! no further updates will be received");
                    return addrs;
                }
                //
                Err(e) => {
                    error!("{}: resolver error: {:?}", self.dst_name, e);
                }
                Ok(Async::Ready(Some(Err(e)))) => {
                    error!("{}: resolver error: {:?}", self.dst_name, e);
                }
                Ok(Async::Ready(Some(Ok(a)))) => {
                    addrs = Some(a);
                }
            }
        }
    }

    fn init_connecting(&mut self) {
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

        let mut rng = rand::thread_rng();
        for _ in 0..needed {
            match select_endpoint(&mut rng, available) {
                None => {
                    trace!("no endpoints ready");
                    self.metrics.unavailable.incr(1);
                    return;
                }
                Some(ep) => {
                    self.metrics.attempts.incr(1);
                    let mut conn = {
                        let sock = self.connector.connect(
                            &ep.peer_addr(),
                            &self.reactor,
                            &self.timer,
                        );
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

    fn dispatch_connected_to_waiters(&mut self) {
        debug!(
            "dispatching {} connections to {} waiters",
            self.connected.len(),
            self.waiters.len()
        );
        while let Some(conn) = self.connected.pop_front() {
            if let Err(conn) = self.dispatch_to_next_waiter(conn) {
                self.connected.push_front(conn);
                return;
            }
        }
    }

    fn dispatch_to_next_waiter(
        &mut self,
        conn: endpoint::Connection,
    ) -> Result<(), endpoint::Connection> {
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
}

/// Buffers up to `max_waiters` concurrent connection requests, along with corresponding
/// connection attempts.
impl<S> Future for Dispatcher<S>
where
    S: Stream<Item = Waiter>,
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        let t0 = Instant::now();

        // Poll all pending connections. Newly established connections are added to the
        // `connected` queue, to be dispatched.
        self.poll_connecting();

        // Now that we may have new established connnections, dispatch them to waiters.
        self.dispatch_connected_to_waiters();

        // Having dispatched, we're ready to refill the waiters queue from the channel. No
        // more than `max_waiters` items are retained at once.
        //
        // We may end up in a situation where we haven't received `Async::NotReady` from
        // `waiters_rx.poll()`. We rely on the fact that connection events will be
        // necessary to satisfy existing waiters, and another `recv_waiters()` call will
        // be triggered from those events.
        self.recv_waiters();

        // Update our lists of endpoints from service discovery before initiating new
        // connections for pending waiters.
        self.update_endpoints();
        self.init_connecting();

        // Dispatch any remaining available connections to any remaining waiters. This is
        // necessary because `init_connecting()` can technically satisfy connections
        // immediately. If this were to happen, there would be gauranteed event to trigger
        // a subsequent dispatch.
        self.dispatch_connected_to_waiters();

        // And because we've potentially drained the waiters queue again, we have to
        // refill it to ensure that this task is polled again.
        self.recv_waiters();

        // Update gauges & record the time it took to poll.
        self.record(t0);

        // This Future never completes.
        // TODO graceful shutdown.
        Ok(Async::NotReady)
    }
}

/// Selects an endpoint using the power of two choices.
///
/// Two endpoints are chosen randomly and return the lesser-loaded endpoint.
/// If no endpoints are available, `None` is retruned.
fn select_endpoint<'r, 'e, R: Rng>(
    rng: &'r mut R,
    available: &'e EndpointMap,
) -> Option<&'e Endpoint> {
    match available.len() {
        0 => None,
        1 => {
            // One endpoint, use it.
            available.get_index(0).map(|(_, ep)| ep)
        }
        sz => {
            // Pick 2 candidate indices.
            let (i0, i1) = if sz == 2 {
                if rng.gen::<bool>() { (0, 1) } else { (1, 0) }
            } else {
                // 3 or more endpoints: choose two distinct endpoints at random.
                let i0 = rng.gen_range(0, sz);
                let mut i1 = rng.gen_range(0, sz);
                while i0 == i1 {
                    i1 = rng.gen_range(0, sz);
                }
                (i0, i1)
            };

            // Determine the the scores of each endpoint
            let (addr0, ep0) = available.get_index(i0).unwrap();
            let (load0, weight0) = (ep0.load(), ep0.weight());
            let score0 = (load0 + 1) as f64 * (1.0 - weight0);

            let (addr1, ep1) = available.get_index(i1).unwrap();
            let (load1, weight1) = (ep1.load(), ep1.weight());
            let score1 = (load1 + 1) as f64 * (1.0 - weight1);

            if score0 <= score1 {
                trace!(
                    "dst: {} {}*{} (not {} {}*{})",
                    addr0,
                    load0,
                    weight0,
                    addr1,
                    load1,
                    weight1
                );
                Some(ep0)
            } else {
                trace!(
                    "dst: {} {}*{} (not {} {}*{})",
                    addr1,
                    load1,
                    weight1,
                    addr0,
                    load0,
                    weight0
                );
                Some(ep1)
            }
        }
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
    fn new(base: &tacho::Scope) -> Metrics {
        let ep = base.clone().prefixed("endpoint");
        let conn = base.clone().prefixed("connection");
        Metrics {
            available: ep.gauge("available"),
            failed: ep.gauge("failed"),
            retired: ep.gauge("retired"),
            pending: conn.gauge("pending"),
            open: conn.gauge("open"),
            waiters: base.gauge("waiters"),
            poll_time: base.timer_us("poll_time_us"),
            unavailable: base.counter("unavailable"),
            attempts: conn.counter("attempts"),
            connects: conn.counter("connects"),
            timeouts: conn.clone().labeled("cause", "timeout").counter("failure"),
            refused: conn.clone().labeled("cause", "refused").counter("failure"),
            failures: conn.clone().labeled("cause", "other").counter("failure"),
            connect_latency: conn.timer_us("latency_us"),
            connection_duration: conn.timer_ms("duration_ms"),
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
