//! Loads a configuration and runs it.

#![allow(missing_docs)]

use super::ConfigError;
use super::connector::ConnectorFactoryConfig;
use super::server::ServerConfig;
use futures::future;
use futures::sync::oneshot;
use serde_json;
use serde_yaml;
use std::cell::RefCell;
use std::rc::Rc;
use tokio_core::reactor::{Core, Handle, Remote};

const DEFAULT_BUFFER_SIZE_BYTES: usize = 16 * 1024;

/// Holds the configuration for a linkerd-tcp instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AppConfig {
    /// The configuration for one or more routers.
    pub routers: Vec<RouterConfig>,

    /// The size of the shared buffer used for transferring data.
    pub buffer_size_bytes: Option<usize>,
}

impl ::std::str::FromStr for AppConfig {
    type Err = ConfigError;

    /// Parses a JSON- or YAML-formatted configuration file.
    fn from_str(txt: &str) -> Result<AppConfig, ConfigError> {
        let txt = txt.trim_left();
        if txt.starts_with('{') {
            serde_json::from_str(txt).map_err(|e| format!("{}", e).into())
        } else {
            serde_yaml::from_str(txt).map_err(|e| format!("{}", e).into())
        }
    }
}

impl AppConfig {
    /// Build an AppSpawner from a configuration.
    pub fn into_app(mut self) -> Result<AppSpawner, ConfigError> {
        let buf = {
            let sz = self.buffer_size_bytes
                .unwrap_or(DEFAULT_BUFFER_SIZE_BYTES);
            Rc::new(RefCell::new(vec![0 as u8; sz]))
        };

        let mut routers = Vec::with_capacity(self.routers.len());
        for config in self.routers.drain(..) {
            let r = config.into_router(buf.clone())?;
            routers.push(r);
        }

        let admin = AdminRunner {};

        Ok(AppSpawner {
               routers: routers,
               admin: admin,
           })
    }
}

pub struct AppSpawner {
    pub routers: Vec<RouterSpawner>,
    pub admin: AdminRunner,
}

/// Holds the configuration for a single stream router.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RouterConfig {
    /// The configuration for one or more servers.
    pub servers: Vec<ServerConfig>,

    /// Determines how outbound connections are initiated.
    ///
    /// By default, connections are clear TCP.
    pub client: Option<ConnectorFactoryConfig>,
}

impl RouterConfig {
    fn into_router(self, _buf: Rc<RefCell<Vec<u8>>>) -> Result<RouterSpawner, ConfigError> {
        Ok(RouterSpawner {})
    }
}

pub struct RouterSpawner {}
impl RouterSpawner {
    pub fn spawn(&self, _reactor: Handle, _admin: Remote) -> Result<(), ConfigError> {
        unimplemented!();
    }
}

pub struct AdminRunner {}
impl AdminRunner {
    pub fn run(&self, closer: oneshot::Sender<()>, mut reactor: Core) -> Result<(), ConfigError> {
        let _ = closer.send(());
        reactor.run(future::ok(()))
    }
}
