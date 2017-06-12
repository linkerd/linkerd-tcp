use super::Balancer;
use super::super::{ConfigError, Path};
use super::super::connector::ConnectorFactory;
use super::super::resolver::Resolve;
use std::cell::RefCell;
use std::rc::Rc;
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

#[derive(Clone)]
pub struct BalancerFactory {
    connector_factory: Rc<RefCell<ConnectorFactory>>,
    metrics: tacho::Scope,
    min_connections: usize,
    max_waiters: usize,
}

impl BalancerFactory {
    pub fn new(min_connections: usize,
               max_waiters: usize,
               cf: ConnectorFactory,
               metrics: &tacho::Scope)
               -> BalancerFactory {
        BalancerFactory {
            min_connections,
            max_waiters,
            connector_factory: Rc::new(RefCell::new(cf)),
            metrics: metrics.clone(),
        }
    }

    pub fn mk_balancer(&self,
                       reactor: &Handle,
                       timer: &Timer,
                       dst_name: &Path,
                       resolve: Resolve)
                       -> Result<Balancer, ConfigError> {
        let connector = self.connector_factory.borrow().mk_connector(dst_name)?;
        let metrics = self.metrics.clone().labeled("dst", dst_name);
        Ok(super::new(reactor,
                      timer,
                      dst_name,
                      self.min_connections,
                      self.max_waiters,
                      connector,
                      resolve,
                      &metrics))
    }
}
