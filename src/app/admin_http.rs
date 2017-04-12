use futures::{Future, future};
use hyper::{self, Get, Post, StatusCode};
use hyper::header::ContentLength;
use hyper::server::{Service, Request, Response};
use std::boxed::Box;
use std::cell::RefCell;
use std::process;
use std::rc::Rc;

pub struct Server {
    prometheus: Rc<RefCell<String>>,
}

impl Server {
    pub fn new(prom: Rc<RefCell<String>>) -> Server {
        Server { prometheus: prom }
    }
    fn get_metrics_body(&self) -> future::FutureResult<String, ()> {
        let prom = self.prometheus.borrow();
        future::ok(prom.clone())
    }
}

impl Service for Server {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;
    fn call(&self, req: Request) -> Self::Future {
        match (req.method(), req.path()) {
            (&Get, "/metrics") => {
                self.get_metrics_body()
                    .then(|body| match body {
                        Ok(body) => {
                            let rsp = Response::new()
                                .with_status(StatusCode::Ok)
                                .with_header(ContentLength(body.len() as u64))
                                .with_body(body);
                            future::ok(rsp)
                        }
                        Err(_) => {
                            future::ok(Response::new().with_status(StatusCode::InternalServerError))
                        }
                    })
                    .boxed()
            }
            (&Post, "/shutdown") => {
                process::exit(0);
            }
            _ => future::ok(Response::new().with_status(StatusCode::NotFound)).boxed(),
        }
    }
}
