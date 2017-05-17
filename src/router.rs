use super::Path;
use super::lb::{Balancer, BalancerFactory};
use super::resolver::{Resolver, Resolve};
use futures::{Future, Stream, Poll, Async};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use tokio_core::reactor::Handle;

/// Produces a `Balancer` for a
///
/// The router maintains an internal cache of routes, by destination name.
#[derive(Clone)]
pub struct Router(Rc<RefCell<InnerRouter>>);

impl Router {
    /// Obtains a balancer for an inbound connection.
    pub fn route(&self, dst: &Path) -> Route {
        self.0.borrow_mut().route(dst)
    }
}

struct InnerRouter {
    reactor: Handle,
    routes: HashMap<Path, Rc<RefCell<Option<RouteState>>>>,
    resolver: Resolver,
    factory: BalancerFactory,
}

impl InnerRouter {
    fn route(&mut self, dst: &Path) -> Route {
        // Try to get a balancer from the cache.
        if let Some(state) = self.routes.get(dst) {
            return Route(Some(state.clone()));
        }

        let new_route = {
            let reactor = self.reactor.clone();
            let resolve = self.resolver.resolve(dst.clone());
            let factory = self.factory.clone();
            let s = RouteState::Pending(reactor, resolve, dst.clone(), factory);
            Rc::new(RefCell::new(Some(s)))
        };
        self.routes.insert(dst.clone(), new_route.clone());
        Route(Some(new_route))
    }
}

enum RouteState {
    Pending(Handle, Resolve, Path, BalancerFactory),
    Ready(Balancer),
}

/// Materializes a `Balancer`.
///
///
#[derive(Clone)]
pub struct Route(Option<Rc<RefCell<Option<RouteState>>>>);

impl Future for Route {
    type Item = Balancer;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let state_ref = self.0.take().expect("route polled after completion");
        let mut state = state_ref.borrow_mut();
        match state.take() {
            None => Err(io::Error::new(io::ErrorKind::Other, "route nullified")),
            Some(RouteState::Pending(reactor, mut resolve, dst, factory)) => {
                match resolve.poll() {
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(()) => Err(io::Error::new(io::ErrorKind::Other, "resolution error")),
                    Ok(Async::Ready(None)) => {
                        Err(io::Error::new(io::ErrorKind::Other,
                                           "resolution stream ended prematurely"))
                    }
                    Ok(Async::Ready(Some(result))) => {
                        match factory.mk_balancer(&dst, result) {
                            Err(e) => {
                                Err(io::Error::new(io::ErrorKind::Other,
                                                   format!("failed to build balancer: {}", e)))
                            }
                            Ok(balancer) => {
                                let updating = resolve
                                    .forward(balancer.clone())
                                    .map(|_| {})
                                    .map_err(|_| {});
                                reactor.spawn(updating);
                                *state = Some(RouteState::Ready(balancer.clone()));
                                Ok(Async::Ready(balancer))
                            }
                        }
                    }
                }
            }
            Some(RouteState::Ready(bal)) => Ok(Async::Ready(bal.clone())),
        }
    }
}
