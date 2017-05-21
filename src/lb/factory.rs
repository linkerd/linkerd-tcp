use super::Balancer;
use super::super::{ConfigError, Path};
use super::super::connector::ConnectorFactory;
use std::cell::RefCell;
use std::rc::Rc;
use tokio_core::reactor::Handle;

#[derive(Clone)]
pub struct BalancerFactory {
    //minimum_connections: usize,
    connector_factory: Rc<RefCell<ConnectorFactory>>,
}

impl BalancerFactory {
    pub fn new(/*min_conns: usize,*/
               cf: ConnectorFactory)
               -> BalancerFactory {
        BalancerFactory {
            //minimum_connections: min_conns,
            connector_factory: Rc::new(RefCell::new(cf)),
        }
    }

    pub fn mk_balancer(&self, reactor: &Handle, dst_name: &Path) -> Result<Balancer, ConfigError> {
        let connector = self.connector_factory.borrow().mk_connector(dst_name)?;
        Ok(Balancer::new(reactor.clone(),
                         dst_name.clone(),
                         //self.minimum_connections,
                         connector))
    }
}
