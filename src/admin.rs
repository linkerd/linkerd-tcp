use super::app::Closer;
use futures::{Future, future};
use hyper::{self, Get, Post, StatusCode};
use hyper::header::ContentLength;
use hyper::server::{Service, Request, Response};
use std::boxed::Box;
use std::cell::RefCell;
use std::process;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

#[derive(Clone)]
pub struct Admin {
    prometheus: Rc<RefCell<String>>,
    closer: Rc<RefCell<Option<Closer>>>,
    grace: Duration,
    reactor: Handle,
    timer: Timer,
}

type RspFuture = Box<Future<Item = Response, Error = hyper::Error>>;

impl Admin {
    pub fn new(
        prometheus: Rc<RefCell<String>>,
        closer: Closer,
        grace: Duration,
        reactor: Handle,
        timer: Timer,
    ) -> Admin {
        Admin {
            closer: Rc::new(RefCell::new(Some(closer))),
            prometheus,
            grace,
            reactor,
            timer,
        }
    }

    fn metrics(&self) -> RspFuture {
        let body = self.prometheus.borrow();
        let rsp = Response::new()
            .with_status(StatusCode::Ok)
            .with_header(ContentLength(body.len() as u64))
            .with_body(body.clone());
        Box::new(future::ok(rsp))
    }

    /// Tell the serving thread to stop what it's doing.
    // TODO offer a `force` param?
    fn shutdown(&self) -> RspFuture {
        let mut closer = self.closer.borrow_mut();
        if let Some(c) = closer.take() {
            info!("shutting down via admin API");
            let _ = c.send(Instant::now() + self.grace);
        }
        let rsp = Response::new().with_status(StatusCode::Ok);
        Box::new(future::ok(rsp))
    }

    fn abort(&self) -> RspFuture {
        process::exit(1);
    }

    fn not_found(&self) -> RspFuture {
        let rsp = Response::new().with_status(StatusCode::NotFound);
        Box::new(future::ok(rsp))
    }
}

impl Service for Admin {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = RspFuture;
    fn call(&self, req: Request) -> RspFuture {
        match (req.method(), req.path()) {
            (&Get, "/metrics") => self.metrics(),
            (&Post, "/shutdown") => self.shutdown(),
            (&Post, "/abort") => self.abort(),
            _ => self.not_found(),
        }
    }
}