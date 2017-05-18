use super::{DstAddr, Path};
use futures::{Future, Sink, Stream, Poll, Async};
use futures::sync::{mpsc, oneshot};
use tokio_core::reactor::Handle;
use tokio_timer::TimerError;

mod namerd;
pub use self::namerd::{Namerd, Addrs};

#[derive(Debug)]
pub enum Error {
    Hyper(::hyper::Error),
    UnexpectedStatus(::hyper::StatusCode),
    Serde(::serde_json::Error),
    Timer(TimerError),
    Rejected,
    NotBound,
}

impl<T> From<mpsc::SendError<T>> for Error {
    fn from(e: mpsc::SendError<T>) -> Error {
        Error::Rejected
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

/// Creates a multithreaded resolver.
///
/// The `Resolver` side is a client of the `Executor`. Namerd work is performed on
/// whatever thread the executor is spawned on.
pub fn new(namerd: Namerd) -> (Resolver, Executor) {
    let (tx, rx) = mpsc::unbounded();
    let res = Resolver { requests: tx };
    let exe = Executor {
        requests: rx,
        namerd: namerd,
    };
    (res, exe)
}

/// Requests resolutions from an `Executor`.
///
/// Resolution requests are sent on an channel along with a response channel. The executor
/// writes to the response channel as results are ready.
#[derive(Clone)]
pub struct Resolver {
    requests: mpsc::UnboundedSender<(Path, mpsc::UnboundedSender<Result<Vec<DstAddr>>>)>,
}

impl Resolver {
    pub fn resolve(&mut self, path: Path) -> Resolve {
        let addrs = {
            let reqs = &self.requests;
            let (tx, rx) = mpsc::unbounded();
            reqs.send((path, tx))
                .expect("failed to send resolution request");
            rx
        };
        Resolve {
            pending: None,
            stream: Some(addrs),
        }
    }
}

/// A stream of name resolutions.
///
/// This stream may not return all resolutions.
pub struct Resolve {
    pending: Option<::std::result::Result<Option<Result<Vec<DstAddr>>>, ()>>,
    stream: Option<mpsc::UnboundedReceiver<Result<Vec<DstAddr>>>>,
}
impl Stream for Resolve {
    type Item = Result<Vec<DstAddr>>;
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Some(pending) = self.pending.take() {
            return match pending {
                       Ok(None) => Ok(Async::Ready(None)),
                       Ok(Some(dsts)) => Ok(Async::Ready(Some(dsts))),
                       Err(e) => Err(e),
                   };
        }

        match self.stream.take() {
            None => Ok(Async::Ready(None)),
            Some(mut stream) => {
                let mut most_recent_result: Option<Result<Vec<DstAddr>>> = None;
                loop {
                    match stream.poll() {
                        Ok(Async::Ready(Some(result))) => {
                            // Note the most recent result
                            most_recent_result = Some(result);
                        }
                        Err(e) => {
                            self.stream = Some(stream);
                            match most_recent_result {
                                None => {
                                    return Err(e);
                                }
                                Some(result) => {
                                    self.pending = Some(Err(e));
                                    return Ok(Async::Ready(Some(result)));
                                }
                            }
                        }
                        Ok(Async::NotReady) => {
                            self.stream = Some(stream);
                            return match most_recent_result {
                                       None => Ok(Async::NotReady),
                                       Some(result) => Ok(Async::Ready(Some(result))),
                                   };
                        }
                        Ok(Async::Ready(None)) => {
                            match most_recent_result {
                                None => {
                                    return Ok(Async::Ready(None));
                                }
                                Some(result) => {
                                    self.pending = Some(Ok(None));
                                    return Ok(Async::Ready(Some(result)));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Serves resolutions from `Resolver`s.
pub struct Executor {
    requests: mpsc::UnboundedReceiver<(Path, mpsc::UnboundedSender<Result<Vec<DstAddr>>>)>,
    namerd: Namerd,
}

impl Executor {
    pub fn execute(self, handle: &Handle) -> Execute {
        let namerd = self.namerd.clone();
        let handle = handle.clone();
        let f = self.requests
            .for_each(move |(path, rsp_tx)| {
                // Stream namerd resolutions to the response channel.
                let resolve = namerd.resolve(&handle, path.as_str());
                let respond = resolve.forward(rsp_tx).map_err(|_| {}).map(|_| {});
                // Do all of this work in another task so that we can receive
                // additional requests.
                handle.clone().spawn(respond);
                Ok(())
            });
        Execute(Box::new(f))
    }
}

// A stream of name resolutions.
pub struct Execute(Box<Future<Item = (), Error = ()>>);
impl Future for Execute {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}
