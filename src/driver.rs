use futures::{Async, AsyncSink, Future, Poll, Sink, Stream};

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
          K: Sink<SinkItem = S::Item, SinkError = S::Error>
{
    pub fn new(src: S, snk: K) -> Driver<S, K> {
        Driver {
            stream: src,
            sink: snk,
            ready: None,
        }
    }

    fn send_ready(&mut self) -> Result<bool, S::Error> {
        trace!("send_ready");
        match self.ready.take() {
            None => {
                trace!("nothing sent; ready!");
                Ok(true)
            }
            Some(item) => {
                trace!("sending");
                match self.sink.start_send(item)? {
                    AsyncSink::Ready => {
                        trace!("ready!");
                        Ok(true)
                    }
                    AsyncSink::NotReady(item) => {
                        trace!("not ready");
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
          K: Sink<SinkItem = S::Item, SinkError = S::Error>
{
    type Item = ();
    type Error = S::Error;
    fn poll(&mut self) -> Poll<(), Self::Error> {
        trace!("polling sink");
        match self.sink.poll_complete()? {
            Async::Ready(_) => trace!("sink ready"),
            Async::NotReady => trace!("sink not ready"),
        }
        loop {
            if self.send_ready()? {
                assert!(self.ready.is_none());
                trace!("polling sink");
                if let Async::Ready(_) = self.sink.poll_complete()? {
                    trace!("sink complete");
                    return Ok(Async::Ready(()));
                }
                trace!("polling stream");
                match self.stream.poll()? {
                    Async::Ready(Some(item)) => {
                        trace!("stream ready");
                        self.ready = Some(item);
                        // Continue trying to send.
                    }
                    Async::Ready(None) => {
                        trace!("stream done");
                        return self.sink.poll_complete();
                    }
                    Async::NotReady => {
                        trace!("stream not ready");
                        return Ok(Async::NotReady);
                    }
                }
            } else {
                trace!("polling sink");
                return self.sink.poll_complete();
            }
        }
    }
}
