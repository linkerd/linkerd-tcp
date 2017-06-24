use super::{dispatchq, Endpoints, EndpointMap, Waiter};
use super::endpoint::{Connection, Endpoint};
use super::super::connector::Connector;
use futures::{Future, Poll, Async, Sink, StartSend};
use rand::{self, Rng};
use std::cell::RefCell;
use std::cmp;
use std::io;
use std::rc::Rc;
use std::time::Instant;
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

pub fn new(
    reactor: Handle,
    timer: Timer,
    connector: Connector,
    endpoints: Endpoints,
    metrics: &tacho::Scope,
) -> Dispatcher {
    let (dispatch_tx, dispatch_rx) = dispatchq::channel(connector.max_waiters());
    Dispatcher {
        reactor,
        timer,
        endpoints,
        dispatch_tx,
        dispatch_rx,
        needed_connections: Rc::new(RefCell::new(connector.min_connections())),
        connector,
        metrics: Metrics::new(metrics),
    }
}

pub struct Dispatcher {
    reactor: Handle,
    timer: Timer,
    connector: Connector,
    endpoints: Endpoints,
    needed_connections: Rc<RefCell<usize>>,
    dispatch_tx: dispatchq::Sender<Waiter>,
    dispatch_rx: dispatchq::Receiver<Waiter>,
    metrics: Metrics,
}

impl Sink for Dispatcher {
    type SinkItem = Waiter;
    type SinkError = ();

    /// Receives and attempts to dispatch new waiters.
    ///
    /// If there are no available connections to be dispatched, up to `max_waiters` are
    /// buffered.
    fn start_send(&mut self, w: Waiter) -> StartSend<Waiter, ()> {
        let res = self.dispatch_tx.start_send(w)?;
        if res.is_ready() {
            *self.needed_connections.borrow_mut() += 1;
        }
        Ok(res)
    }

    fn poll_complete(&mut self) -> Poll<(), ()> {
        let t0 = Instant::now();
        debug!("poll_complete: connecting");
        let connecting_ready = self.init_connecting().is_ready();
        let dispatch_ready = self.dispatch_tx.poll_complete()?.is_ready();
        self.record(t0);
        debug!(
            "poll_complete: dispatch={} connect={}",
            dispatch_ready,
            connecting_ready
        );
        if dispatch_ready && connecting_ready {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl Dispatcher {
    fn init_connecting(&mut self) -> Async<()> {
        let needed = {
            let space = self.dispatch_tx.available_capacity();
            cmp::min(*self.needed_connections.borrow(), space)
        };
        debug!("initiating {} connections", needed);
        if needed == 0 {
            return Async::Ready(());
        }

        let available = self.endpoints.updated_available();
        trace!("{} endpoints available", available.len());
        if available.is_empty() {
            return Async::NotReady;
        }

        let mut rng = rand::thread_rng();
        for i in 0..needed {
            debug!("dispatching {}/{}", i + 1, needed);
            match Dispatcher::select_endpoint(&mut rng, available) {
                None => {
                    trace!("no endpoints ready");
                    self.metrics.unavailable.incr(1);
                    return Async::NotReady;
                }
                Some(ep) => {
                    self.metrics.attempts.incr(1);
                    let conn = {
                        let sock = self.connector.connect(
                            &ep.peer_addr(),
                            &self.reactor,
                            &self.timer,
                        );
                        let c = ep.connect(sock, &self.metrics.connection_duration);
                        self.metrics.connect_latency.time(c)
                    };
                    Dispatcher::dispatch(
                        conn,
                        &self.reactor,
                        self.dispatch_rx.clone(),
                        self.needed_connections.clone(),
                        self.metrics.connects.clone(),
                        self.metrics.failures.clone(),
                    );
                }
            }
        }
        Async::Ready(())
    }

    fn dispatch<C>(
        conn: C,
        reactor: &Handle,
        dispatch_rx: dispatchq::Receiver<Waiter>,
        needed_connections: Rc<RefCell<usize>>,
        connects: tacho::Counter,
        failures: Failures,
    ) where
        C: Future<Item = Connection, Error = io::Error> + 'static,
    {
        let task = conn.map_err(move |e| {
            error!("connection error: {}", e);
            failures.record(&e);
        }).and_then(move |dst| {
                debug!("receiving inbound waiter for {}", dst.peer_addr());
                let tx = dispatch_rx.recv().map_err(|_| {}).and_then(move |w| {
                    trace!("dispatching outbound to waiter");
                    connects.incr(1);
                    *needed_connections.borrow_mut() -= 1;
                    w.send(dst).map_err(|_| {})
                });
                tx.map(|_| trace!("dispatched outbound to waiter"))
                    .map_err(|_| warn!("dropped outbound connection"))
            });
        reactor.spawn(task)
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

    fn record(&self, t0: Instant) {
        self.metrics.waiters.set(self.dispatch_tx.len());
        self.metrics.poll_time.record_since(t0);
    }
}

struct Metrics {
    waiters: tacho::Gauge,
    attempts: tacho::Counter,
    unavailable: tacho::Counter,
    failures: Failures,
    connects: tacho::Counter,
    connect_latency: tacho::Timer,
    connection_duration: tacho::Timer,
    poll_time: tacho::Timer,
}

impl Metrics {
    fn new(base: &tacho::Scope) -> Metrics {
        let conn = base.clone().prefixed("connection");
        let dspt = base.clone().prefixed("dispatch");
        Metrics {
            attempts: conn.counter("attempts"),
            connects: conn.counter("connects"),
            connect_latency: conn.timer_us("latency_us"),
            connection_duration: conn.timer_ms("duration_ms"),
            failures: Failures {
                timeouts: conn.clone().labeled("cause", "timeout").counter("failure"),
                refused: conn.clone().labeled("cause", "refused").counter("failure"),
                reset: conn.clone().labeled("cause", "reset").counter("failure"),
                other: conn.clone().labeled("cause", "other").counter("failure"),
            },

            poll_time: dspt.timer_us("poll_time_us"),
            unavailable: dspt.counter("unavailable"),
            waiters: dspt.gauge("waiters"),
        }
    }
}

#[derive(Clone)]
struct Failures {
    timeouts: tacho::Counter,
    refused: tacho::Counter,
    reset: tacho::Counter,
    other: tacho::Counter,
}
impl Failures {
    fn record(&self, err: &io::Error) {
        match err.kind() {
            io::ErrorKind::TimedOut => self.timeouts.incr(1),
            io::ErrorKind::ConnectionRefused => self.refused.incr(1),
            io::ErrorKind::ConnectionReset => self.reset.incr(1),
            _ => self.other.incr(1),
        }
    }
}
