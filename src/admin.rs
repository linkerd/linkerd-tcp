use super::app::Closer;
use futures::{Future, future};
use hyper::{self, Get, Post, StatusCode};
use hyper::header::ContentLength;
use hyper::server::{Service, Request, Response};
use std::boxed::Box;
use std::cell::RefCell;
//use std::process;
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

impl Admin {
    pub fn new(prometheus: Rc<RefCell<String>>,
               closer: Closer,
               grace: Duration,
               reactor: Handle,
               timer: Timer)
               -> Admin {
        Admin {
            closer: Rc::new(RefCell::new(Some(closer))),
            prometheus,
            grace,
            reactor,
            timer,
        }
    }
}

impl Service for Admin {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;
    fn call(&self, req: Request) -> Self::Future {
        match (req.method(), req.path()) {
            (&Get, "/metrics") => {
                let body = self.prometheus.borrow();
                let rsp = Response::new()
                    .with_status(StatusCode::Ok)
                    .with_header(ContentLength(body.len() as u64))
                    .with_body(body.clone());
                future::ok(rsp).boxed()
            }
            (&Post, "/shutdown") => {
                {
                    let mut closer = self.closer.borrow_mut();
                    if let Some(c) = closer.take() {
                        info!("shutting down via admin API");
                        if c.send(Instant::now() + self.grace).is_err() {
                            debug!("closer not being waited upon");
                        }
                    }
                }
                // let force = self.timer
                //     .sleep(self.grace + Duration::from_millis(1))
                //     .then(|_| {
                //               println!("forcefully exiting the process");
                //               process::exit(0);
                //           })
                //     .map_err(|_| {})
                //     .map(|_| {});
                // self.reactor.spawn(force);

                let rsp = Response::new().with_status(StatusCode::Ok);
                future::ok(rsp).boxed()
            }
            _ => {
                let rsp = Response::new().with_status(StatusCode::NotFound);
                future::ok(rsp).boxed()
            }
        }
    }
}
