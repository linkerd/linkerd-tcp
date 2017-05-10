use super::{balancer, Envelope, Path, Resolver, Resolve};
use futures::{Future, Stream, Poll, Async};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use tokio_core::reactor::Handle;

/// Routes incoming connections to an outbound balancer.
#[derive(Clone, Debug)]
pub struct Router {
    reactor: Handle,
    routes: Rc<RefCell<HashMap<Path, Route>>>,
    resolver: Resolver,
}
impl Router {
    pub fn route(&mut self, env: &Envelope) -> Route {
        let mut routes = self.routes.borrow_mut();

        // Try to get a balancer from the cache.
        if let Some(bal) = routes.get(&env.dst_name) {
            return (*bal).clone();
        }

        let resolve = self.resolver.resolve(env.dst_name.clone());
        let route = Route {
            reactor: self.reactor.clone(),
            resolve: Some(resolve),
        };
        routes.insert(env.dst_name.clone(), route.clone());
        route
    }
}

/// Materializes a load balancer from a resolution stream.
///
///
#[derive(Clone, Debug)]
pub struct Route {
    reactor: Handle,
    resolve: Option<Resolve>,
}
impl Future for Route {
    type Item = balancer::Balancer;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.resolve.take() {
            None => panic!("polled after completion"),
            Some(mut resolve) => {
                match resolve.poll()? {
                    Async::Ready(None) => {
                        Err(io::Error::new(io::ErrorKind::Other,
                                           "resolution stream ended prematurely"))
                    }
                    Async::NotReady => {
                        self.resolve = Some(resolve);
                        Ok(Async::NotReady)
                    }
                    Async::Ready(Some(addr)) => {
                        let bal = balancer::new(addr);

                        let updating = resolve.forward(bal.clone()).map(|_| {}).map_err(|_| {});
                        self.reactor.spawn(updating);

                        Ok(Async::Ready(bal))
                    }
                }
            }
        }
    }
}
