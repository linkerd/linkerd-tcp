use super::Balancer;
use super::super::Path;
use super::super::connector::{ConfigError, ConnectorFactory};
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
}

impl BalancerFactory {
    pub fn new(cf: ConnectorFactory, metrics: &tacho::Scope) -> BalancerFactory {
        BalancerFactory {
            connector_factory: Rc::new(RefCell::new(cf)),
            metrics: metrics.clone(),
        }
    }

    pub fn mk_balancer(
        &self,
        reactor: &Handle,
        timer: &Timer,
        dst_name: &Path,
        resolve: Resolve,
    ) -> Result<Balancer, ConfigError> {
        let connector = self.connector_factory.borrow().mk_connector(dst_name)?;
        let metrics = self.metrics.clone().labeled("dst", dst_name);
        Ok(super::new(
            reactor,
            timer,
            dst_name,
            connector,
            resolve,
            &metrics,
        ))
    }
}
