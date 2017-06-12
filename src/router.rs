use super::{ConfigError, Path};
use super::balancer::{Balancer, BalancerFactory};
use super::resolver::Resolver;
use futures::{Future, Poll, Async};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use tacho::{self, Timing};
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

static ROUTE_CREATE_KEY: &'static str = "route_create";
static ROUTE_ERROR_KEY: &'static str = "route_error";
static ROUTE_FOUND_KEY: &'static str = "route_found";
static ROUTE_TIME_US_KEY: &'static str = "route_time_us";

pub fn new(resolver: Resolver, factory: BalancerFactory, metrics: &tacho::Scope) -> Router {
    let inner = InnerRouter {
        resolver,
        factory,
        routes: HashMap::default(),
        route_create: metrics.counter(ROUTE_CREATE_KEY),
        route_error: metrics.counter(ROUTE_ERROR_KEY),
        route_found: metrics.counter(ROUTE_FOUND_KEY),
        route_time_us: metrics.stat(ROUTE_TIME_US_KEY),
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
    routes: HashMap<Path, Balancer>,
    resolver: Resolver,
    factory: BalancerFactory,
    route_create: tacho::Counter,
    route_error: tacho::Counter,
    route_found: tacho::Counter,
    route_time_us: tacho::Stat,
}

impl InnerRouter {
    fn route(&mut self, dst: &Path, reactor: &Handle, timer: &Timer) -> Route {
        let t = tacho::Timing::start();
        let r = self.do_route(dst, reactor, timer);
        self.route_time_us.add(t.elapsed_us());
        Route(Some(r))
    }

    fn do_route(&mut self,
                dst: &Path,
                reactor: &Handle,
                timer: &Timer)
                -> Result<Balancer, ConfigError> {
        // Try to get a balancer from the cache.
        if let Some(route) = self.routes.get(dst) {
            self.route_found.incr(1);
            return Ok(route.clone());
        }

        let resolve = self.resolver.resolve(dst.clone());
        match self.factory.mk_balancer(reactor, timer, dst, resolve) {
            Err(e) => {
                self.route_error.incr(1);
                Err(e)
            }
            Ok(balancer) => {
                self.routes.insert(dst.clone(), balancer.clone());
                Ok(balancer)
            }
        }
    }
}

/// Materializes a `Balancer`.
///
///
#[derive(Clone)]
pub struct Route(Option<Result<Balancer, ConfigError>>);
impl Future for Route {
    type Item = Balancer;
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
