use super::{ConfigError, Path};
use super::lb::{Balancer, BalancerFactory, Dispatcher, Dispatch};
use super::resolver::{Resolver, Resolve};
use futures::{Future, Stream, Poll, Async};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use tokio_core::reactor::Handle;

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
    pub fn route(&self, dst: &Path, rct: &Handle) -> Route {
        self.0.borrow_mut().route(dst, rct)
    }
}

struct InnerRouter {
    routes: HashMap<Path, Dispatcher>,
    resolver: Resolver,
    factory: BalancerFactory,
}

impl InnerRouter {
    fn route(&mut self, dst: &Path, reactor: &Handle) -> Route {
        // Try to get a balancer from the cache.
        if let Some(route) = self.routes.get(dst) {
            return Route(Some(Ok(route.clone())));
        }

        match self.factory.mk_balancer(&reactor, &dst) {
            Err(e) => Route(Some(Err(e))),
            Ok(Balancer {
                   dispatcher,
                   manager,
               }) => {
                let resolve = self.resolver.resolve(dst.clone());
                reactor.spawn(manager.manage(resolve));

                self.routes.insert(dst.clone(), dispatcher.clone());
                Route(Some(Ok(dispatcher)))
            }
        }
    }
}

enum RouteState {
    Pending(Handle, Resolve, Path, Balancer),
    Ready(Dispatcher),
}

/// Materializes a `Balancer`.
///
///
#[derive(Clone)]
pub struct Route(Option<Result<Dispatcher, ConfigError>>);
impl Future for Route {
    type Item = Dispatcher;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0
                  .take()
                  .expect("route must not be polled more than once") {
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("config error: {}", e))),
            Ok(dispatcher) => Ok(Async::Ready(dispatcher)),
        }
    }
}
