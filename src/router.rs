use super::Path;
use super::balancer::{Balancer, BalancerConfig, BalancerFactory};
use super::client::Client;
use super::connection::ConnectionCtx;
use super::resolver::{Resolver, Resolve};
use super::server::ServerCtx;
use futures::{Future, Stream, Poll, Async};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use tokio_core::reactor::Handle;

/// Routes incoming connections to an outbound balancer.
#[derive(Clone)]
pub struct Router {
    reactor: Handle,
    routes: Rc<RefCell<HashMap<Path, Route>>>,
    resolver: Resolver,
    factory: BalancerFactory,
}
impl Router {
    pub fn route(&mut self, ctx: &ConnectionCtx<ServerCtx>) -> Route {
        let mut routes = self.routes.borrow_mut();

        // Try to get a balancer from the cache.
        let dst = ctx.dst_name();
        if let Some(bal) = routes.get(dst) {
            return (*bal).clone();
        }

        let resolve = self.resolver.resolve(dst.clone());
        let balancer = self.factory.mk_balancer(dst);
        let route = Route(RouteState::Pending(self.reactor.clone(), resolve, balancer));
        routes.insert(dst.clone(), route.clone());
        route
    }
}

/// Materializes a load balancer from a resolution stream.
///
#[derive(Clone)]
pub struct Route(RouteState);
#[derive(Clone)]
enum RouteState {
    Pending(Handle, Resolve, Balancer),
    Ready(Balancer),
}
impl Future for Route {
    type Item = Balancer;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0 {
            RouteState::Ready(bal) => Ok(Async::Ready(bal.clone())),
            RouteState::Pending(reactor, mut resolve, factory) => {
                match resolve.poll() {
                    Err(_) => Err(io::Error::new(io::ErrorKind::Other, "resolution error")),
                    Ok(Async::Ready(None)) => {
                        Err(io::Error::new(io::ErrorKind::Other,
                                           "resolution stream ended prematurely"))
                    }
                    Ok(Async::NotReady) => {
                        self.0 = RouteState::Pending(reactor, resolve, factory);
                        Ok(Async::NotReady)
                    }
                    Ok(Async::Ready(Some(res))) => {
                        let balancer = factory.build(res);
                        let updating = resolve
                            .forward(balancer.clone())
                            .map(|_| {})
                            .map_err(|_| {});
                        reactor.spawn(updating);
                        self.0 = RouteState::Ready(balancer.clone());
                        Ok(Async::Ready(balancer))
                    }
                }
            }
        }
    }
}
