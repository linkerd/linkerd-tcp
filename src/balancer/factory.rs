use super::Balancer;
use super::super::{ConfigError, Path};
use super::super::connector::ConnectorFactory;
use std::cell::RefCell;
use std::rc::Rc;
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

#[derive(Clone)]
pub struct BalancerFactory {
    //minimum_connections: usize,
    connector_factory: Rc<RefCell<ConnectorFactory>>,
    metrics: tacho::Scope,
}

impl BalancerFactory {
    pub fn new(/*min_conns: usize,*/
               cf: ConnectorFactory,
               metrics: &tacho::Scope)
               -> BalancerFactory {
        BalancerFactory {
            //minimum_connections: min_conns,
            connector_factory: Rc::new(RefCell::new(cf)),
            metrics: metrics.clone(),
        }
    }

    pub fn mk_balancer(&self,
                       reactor: &Handle,
                       timer: &Timer,
                       dst_name: &Path)
                       -> Result<Balancer, ConfigError> {
        let connector = self.connector_factory.borrow().mk_connector(dst_name)?;
        Ok(Balancer::new(reactor,
                         timer,
                         dst_name,
                         //self.minimum_connections,
                         connector))
    }
}
