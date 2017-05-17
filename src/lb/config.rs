use super::factory::{self, BalancerFactory};
use super::super::ConfigError;
use super::super::connector::ConnectorFactoryConfig;
use std::time;
use tokio_core::reactor::Handle;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BalancerConfig {
    pub minimum_connections: usize,
    pub maximum_waiters: usize,
    pub client: ConnectorFactoryConfig,
}

impl BalancerConfig {
    pub fn mk_factory(&self, handle: &Handle) -> Result<BalancerFactory, ConfigError> {
        let cf = self.client.mk_connector_factory(handle)?;
        Ok(factory::new(self.minimum_connections, self.maximum_waiters, cf))
    }
}
