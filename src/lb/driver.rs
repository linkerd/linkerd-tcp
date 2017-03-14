use futures::{Async, AsyncSink, Future, Poll, Sink, Stream};
use std::fmt;

/// This is similar to `futures::stream::Forwar ` but also calls
/// `poll_complete` on wakeups. This is important to keep connection
/// pool up to date when no new requests are coming in.
///
// Borrowed from tk-pool.
pub struct Driver<S: Stream, K: Sink<SinkItem = S::Item>> {
    stream: S,
    sink: K,
    ready: Option<S::Item>,
}

impl<S, K> Driver<S, K>
    where S: Stream,
          K: Sink<SinkItem = S::Item>,
          K::SinkError: fmt::Display
{
    pub fn new(src: S, snk: K) -> Driver<S, K> {
        Driver {
            stream: src,
            sink: snk,
            ready: None,
        }
    }

    fn send_ready(&mut self) -> Result<bool, ()> {
        match self.ready.take() {
            None => Ok(true),
            Some(item) => {
                debug!("offering an upstream connection downstream");
                let send = self.sink
                    .start_send(item)
                    .map_err(|e| error!("Failed to send item to sink: {}", e));
                match send? {
                    AsyncSink::Ready => {
                        debug!("downstream is ready");
                        Ok(true)
                    }
                    AsyncSink::NotReady(item) => {
                        debug!("downstream not ready");
                        self.ready = Some(item);
                        Ok(false)
                    }
                }
            }
        }
    }
}

/// A Future that is complete when the stream has been fully flushed
/// into the sink. Ensures that the sink's `poll_complete` is called
/// aggressively to
impl<S, K> Future for Driver<S, K>
    where S: Stream,
          K: Sink<SinkItem = S::Item>,
          K::SinkError: fmt::Display
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), Self::Error> {
        debug!("polling");
        self.sink
            .poll_complete()
            .map_err(|e| error!("Failed to poll sink: {}", e))?;
        loop {
            if self.send_ready()? {
                assert!(self.ready.is_none());
                {
                    let done = self.sink
                        .poll_complete()
                        .map_err(|e| error!("Failed to poll sink: {}", e));
                    if let Async::Ready(_) = done? {
                        return Ok(Async::Ready(()));
                    }
                }
                {
                    let ready = self.stream
                        .poll()
                        .map_err(|_| error!("Failed to poll stream"));
                    match ready? {
                        Async::Ready(Some(item)) => {
                            self.ready = Some(item);
                            // Continue trying to send.
                        }
                        Async::Ready(None) => {
                            return self.sink
                                .poll_complete()
                                .map_err(|e| error!("Failed to poll sink: {}", e));
                        }
                        Async::NotReady => return Ok(Async::NotReady),
                    }
                }
            } else {
                return self.sink.poll_complete().map_err(|e| error!("Failed to poll sink: {}", e));
            }
        }
    }
}
