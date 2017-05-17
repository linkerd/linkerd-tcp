use super::{DstAddr, Path};
use futures::{Future, Sink, Stream, Poll};
use futures::sync::{mpsc, oneshot};
use tokio_core::reactor::Handle;
use tokio_timer::TimerError;

mod namerd;
use self::namerd::{Namerd, Addrs};

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

// TODO In the future, we likely want to change this to use the split bind & addr APIs so
// balancers can be shared across logical names. In the meantime, it's sufficient to have
// a balancer per logical name.
#[derive(Clone)]
pub struct Resolver {
    requests: mpsc::UnboundedSender<(Path, mpsc::UnboundedSender<Result<Vec<DstAddr>>>)>,
}

impl Resolver {
    pub fn resolve(&mut self, path: Path) -> Resolve {
        let addrs = {
            let mut reqs = &self.requests;
            let (tx, rx) = mpsc::unbounded();
            reqs.send((path, tx));
            rx
        };
        Resolve(addrs)
    }
}

pub struct Executor {
    requests: mpsc::UnboundedReceiver<(Path, mpsc::UnboundedSender<Result<Vec<DstAddr>>>)>,
    namerd: Namerd,
}
impl Executor {
    pub fn execute(self, handle: &Handle) -> Box<Future<Item = (), Error = ()>> {
        let namerd = self.namerd.clone();
        let handle = handle.clone();
        let f = self.requests
            .for_each(move |(path, rsp_tx)| {
                          let resolve = namerd.resolve(&handle, path.as_str());
                          let respond = resolve.forward(rsp_tx).map_err(|_| {}).map(|_| {});
                          handle.clone().spawn(respond);
                          Ok(())
                      });
        Box::new(f)
    }
}

// A stream of name resolutions.
pub struct Resolve(mpsc::UnboundedReceiver<Result<Vec<DstAddr>>>);
impl Stream for Resolve {
    type Item = Result<Vec<DstAddr>>;
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.poll()
    }
}
