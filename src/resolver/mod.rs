use super::{WeightedAddr, Path};
use futures::{Future, Stream, Poll};
use futures::sync::mpsc;
use tokio_core::reactor::Handle;
use tokio_timer::{Timer, TimerError};

mod config;
mod namer;
pub use self::config::{Error as ConfigError, ConstantConfig, NamerdConfig};
pub use self::namer::Namer;

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
    fn from(_e: mpsc::SendError<T>) -> Error {
        Error::Rejected
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

/// Creates a multithreaded resolver.
///
/// The `Resolver` side is a client of the `Executor`. Namer work is performed on
/// whatever thread the executor is spawned on.
pub fn new(namer: Namer) -> (Resolver, Executor) {
    let (tx, rx) = mpsc::unbounded();
    let res = Resolver { requests: tx };
    let exe = Executor {
        requests: rx,
        namer: namer,
    };
    (res, exe)
}

/// Requests resolutions from an `Executor`.
///
/// Resolution requests are sent on an channel along with a response channel. The executor
/// writes to the response channel as results are ready.
#[derive(Clone)]
pub struct Resolver {
    requests: mpsc::UnboundedSender<(Path, mpsc::UnboundedSender<Result<Vec<WeightedAddr>>>)>,
}

impl Resolver {
    pub fn resolve(&mut self, path: Path) -> Resolve {
        let addrs = {
            let reqs = &self.requests;
            let (tx, rx) = mpsc::unbounded();
            reqs.send((path, tx)).expect(
                "failed to send resolution request",
            );
            rx
        };
        Resolve(addrs)
    }
}

pub struct Resolve(mpsc::UnboundedReceiver<Result<Vec<WeightedAddr>>>);

impl Stream for Resolve {
    type Item = Result<Vec<WeightedAddr>>;
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.poll()
    }
}

/// Serves resolutions from `Resolver`s.
pub struct Executor {
    requests: mpsc::UnboundedReceiver<(Path, mpsc::UnboundedSender<Result<Vec<WeightedAddr>>>)>,
    namer: Namer,
}

impl Executor {
    pub fn execute(self, handle: &Handle, timer: &Timer) -> Execute {
        let handle = handle.clone();
        let namer = self.namer.with_handle(&handle, timer);
        let f = self.requests.for_each(move |(path, rsp_tx)| {
            // Stream namer resolutions to the response channel.
            let resolve = namer.resolve(path.as_str());
            let respond = resolve.forward(rsp_tx).map_err(|_| {}).map(|_| {});
            // Do all of this work in another task so that we can receive
            // additional requests.
            handle.spawn(respond);
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
