use super::{ConfigError, Path};
use super::balancer::{Balancer, BalancerFactory, Selector};
use super::resolver::Resolver;
use futures::{Future, Poll, Async};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

pub fn new(resolver: Resolver, factory: BalancerFactory) -> Router {
    let inner = InnerRouter {
        routes: HashMap::default(),
        resolver: resolver,
        factory: factory,
    };
    Router(Rc::new(RefCell::new(inner)))
}


/// Produces a `Balancer` for a
///
/// The router maintains an internal cache of routes, by destination name.
#[derive(Clone)]
pub struct Router(Rc<RefCell<InnerRouter>>);

impl Router {
    /// Obtains a balancer for an inbound connection.
    pub fn route(&self, dst: &Path, rct: &Handle, tim: &Timer) -> Route {
        self.0.borrow_mut().route(dst, rct, tim)
    }
}

struct InnerRouter {
    routes: HashMap<Path, Selector>,
    resolver: Resolver,
    factory: BalancerFactory,
}

impl InnerRouter {
    fn route(&mut self, dst: &Path, reactor: &Handle, timer: &Timer) -> Route {
        // Try to get a balancer from the cache.
        if let Some(route) = self.routes.get(dst) {
            return Route(Some(Ok(route.clone())));
        }

        match self.factory.mk_balancer(reactor, timer, dst) {
            Err(e) => Route(Some(Err(e))),
            Ok(Balancer { selector, manager }) => {
                let resolve = self.resolver.resolve(dst.clone());
                reactor.spawn(manager.manage(resolve).map_err(|_| {}));

                self.routes.insert(dst.clone(), selector.clone());
                Route(Some(Ok(selector)))
            }
        }
    }
}

/// Materializes a `Balancer`.
///
///
#[derive(Clone)]
pub struct Route(Option<Result<Selector, ConfigError>>);
impl Future for Route {
    type Item = Selector;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0
                  .take()
                  .expect("route must not be polled more than once") {
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("config error: {}", e))),
            Ok(selector) => Ok(Async::Ready(selector)),
        }
    }
}
