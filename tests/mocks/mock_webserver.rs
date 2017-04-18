extern crate futures;
extern crate hyper;

use futures::{Future, future};
use hyper::{Get, StatusCode};
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Service, Request, Response};
use std::boxed::Box;

pub struct MockWebServer {}

impl Default for MockWebServer {
    fn default() -> MockWebServer {
        MockWebServer {}
    }
}

impl Service for MockWebServer {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;
    fn call(&self, req: Request) -> Self::Future {
        match (req.method(), req.path()) {
            (&Get, "/") => {
                println!("mock webserver received request");
                let body = "{\"hello\":\"world\"}".to_owned();
                let rsp = Response::new()
                    .with_status(StatusCode::Ok)
                    .with_header(ContentType::json())
                    .with_header(ContentLength(body.len() as u64))
                    .with_body(body);
                future::ok(rsp).boxed()
            }
            _ => future::ok(Response::new().with_status(StatusCode::NotFound)).boxed(),
        }
    }
}
