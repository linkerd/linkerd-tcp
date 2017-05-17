use super::DstAddr;
use super::balancer::{self, Balancer};
use super::super::{ConfigError, Path, resolver};
use super::super::connector::ConnectorFactory;
use std::cell::RefCell;
use std::rc::Rc;
use tokio_core::reactor::Handle;

#[derive(Clone)]
pub struct BalancerFactory {
    minimum_connections: usize,
    maximum_waiters: usize,
    connector_factory: Rc<RefCell<ConnectorFactory>>,
}

impl BalancerFactory {
    pub fn new(min_conns: usize, max_waiters: usize, cf: ConnectorFactory) -> BalancerFactory {
        BalancerFactory {
            minimum_connections: min_conns,
            maximum_waiters: max_waiters,
            connector_factory: Rc::new(RefCell::new(cf)),
        }
    }

    pub fn mk_balancer(&self,
                       reactor: &Handle,
                       dst_name: &Path,
                       init: resolver::Result<Vec<DstAddr>>)
                       -> Result<Balancer, ConfigError> {
        let connector = self.connector_factory.borrow().mk_connector(dst_name)?;

        let b = balancer::new(reactor.clone(),
                              dst_name.clone(),
                              self.minimum_connections,
                              self.maximum_waiters,
                              connector,
                              init);
        Ok(b)
    }
}
