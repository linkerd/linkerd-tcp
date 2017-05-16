use super::connection::{Connection, Duplex, Summary};
use super::lb::Connect;
use super::router::{Router, Route};
use futures::{Future, Poll, Async};
use std::{io, net};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
//use tacho::Scope;

mod serving;
use self::serving::Serving;

/// An incoming connection.
pub type SrcConnection = Connection<ServerCtx>;

pub fn new(router: Router, buf: Rc<RefCell<Vec<u8>>>) -> Server {
    Server {
        router: router,
        buf: buf,
    }
}

pub struct Server {
    router: Router,
    buf: Rc<RefCell<Vec<u8>>>,
    // srv_metrics: tacho::Scope,
    // metrics: tacho::Scope,
}
impl Server {
    pub fn serve(&mut self, src: Connection<ServerCtx>) -> Serving {
        let route = self.router.route(src.context.dst_name());
        serving::new(src, self.buf.clone(), route)
    }
}

/// The state of a
#[derive(Clone, Debug, Default)]
pub struct ServerCtx(Rc<RefCell<InnerServerCtx>>);

#[derive(Debug, Default)]
struct InnerServerCtx {
    connects: usize,
    disconnects: usize,
    failures: usize,
    bytes_to_dst: usize,
    bytes_to_src: usize,
}

impl ServerCtx {
    fn active(&self) -> usize {
        let InnerServerCtx {
            connects,
            disconnects,
            ..
        } = *self.0.borrow();

        connects - disconnects
    }
}
