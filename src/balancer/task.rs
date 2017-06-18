use super::{Endpoints, Waiter};
use super::dispatcher::{Dispatcher, State as DispatchState};
use super::super::Path;
use super::super::connector::Connector;
use super::super::resolver::Resolve;
use futures::{Future, Stream, Poll, Async};
use std::io;
use std::time::Instant;
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

pub fn new<S>(reactor: Handle,
              timer: Timer,
              dst_name: Path,
              connector: Connector,
              resolve: Resolve,
              waiters_rx: S,
              metrics: &tacho::Scope)
              -> Task<S>
    where S: Stream<Item = Waiter>
{
    let endpoints = Endpoints::new(dst_name.clone(),
                                   resolve,
                                   connector.failure_limit(),
                                   connector.failure_penalty(),
                                   &metrics.clone().prefixed("endpoint"));
    let d = Dispatcher::new(reactor, timer, dst_name, waiters_rx, connector, metrics);
    Task {
        endpoints,
        dispatcher: Some(d),
        poll_time: metrics.timer_us("poll_time_us"),
    }
}

/// A background task that satisfies connection requests.
pub struct Task<W> {
    /// Holds the state of all available/failed/retired endpoints.
    endpoints: Endpoints,

    // Holds the state of all connections and waiters.
    dispatcher: Option<Dispatcher<W>>,

    poll_time: tacho::Timer,
}

/// Buffers up to `max_waiters` concurrent connection requests, along with corresponding
/// connection attempts.
impl<S> Future for Task<S>
    where S: Stream<Item = Waiter>
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        if let Some(mut dispatcher) = self.dispatcher.take() {
            let t0 = Instant::now();

            let mut state = DispatchState::NeedsPoll;
            while state == DispatchState::NeedsPoll {
                state = match dispatcher.poll() {
                    s @ DispatchState::Done => s,
                    _ => {
                        let available = self.endpoints.updated_available();
                        dispatcher.init(available)
                    }
                };
            }

            trace!("dispatcher state: {:?}", state);
            if state != DispatchState::Done {
                self.dispatcher = Some(dispatcher);
            }

            self.poll_time.record_since(t0);
        }

        // This future completes once the waiters stream has ended.
        if self.dispatcher.is_none() {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}
