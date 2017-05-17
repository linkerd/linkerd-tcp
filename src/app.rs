//! Loads a configuration and runs it.

#![allow(missing_docs)]

use super::{resolver, router, server};
use super::ConfigError;
use super::connector::ConnectorFactoryConfig;
use super::lb::BalancerFactory;
use futures::future;
use futures::sync::oneshot;
use serde_json;
use serde_yaml;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time;
use tacho;
use tokio_core::reactor::{Core, Handle};

const DEFAULT_BUFFER_SIZE_BYTES: usize = 16 * 1024;
const DEFAULT_MINIMUM_CONNECTIONS: usize = 1;
const DEFAULT_MAXIMUM_WAITERS: usize = 128;

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

        let (metrics_tx, metrics_rx) = tacho::new();

        let mut routers = VecDeque::with_capacity(self.routers.len());
        let mut resolvers = VecDeque::with_capacity(self.routers.len());
        for config in self.routers.drain(..) {
            let mut r = config.into_router(buf.clone(), metrics_tx.clone())?;
            let e = r.resolver_executor
                .take()
                .expect("router missing resolver executor");
            routers.push_back(r);
            resolvers.push_back(e);
        }

        let admin = AdminRunner {
            metrics: metrics_rx,
            resolvers: resolvers,
        };

        Ok(AppSpawner {
               routers: routers,
               admin: admin,
           })
    }
}

pub struct AppSpawner {
    pub routers: VecDeque<RouterSpawner>,
    pub admin: AdminRunner,
}

/// Holds the configuration for a single stream router.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RouterConfig {
    /// The configuration for one or more servers.
    pub servers: Vec<server::ServerConfig>,

    /// Determines how outbound connections are initiated.
    ///
    /// By default, connections are clear TCP.
    pub client: Option<ConnectorFactoryConfig>,

    pub minimum_connections: Option<usize>,
    pub maximum_waiters: Option<usize>,
}

impl RouterConfig {
    fn into_router(mut self,
                   buf: Rc<RefCell<Vec<u8>>>,
                   metrics: tacho::Scope)
                   -> Result<RouterSpawner, ConfigError> {
        let n = resolver::Namerd::new("http://localhost:4180".into(),
                                      time::Duration::from_secs(10),
                                      "default".into(),
                                      metrics);
        let (resolver, resolver_exec) = resolver::new(n);

        let balancer = {
            let min_conns = self.minimum_connections
                .unwrap_or(DEFAULT_MINIMUM_CONNECTIONS);
            let max_waiters = self.maximum_waiters.unwrap_or(DEFAULT_MAXIMUM_WAITERS);
            let client = self.client.unwrap_or_default().mk_connector_factory()?;
            BalancerFactory::new(min_conns, max_waiters, client)
        };
        let router = router::new(resolver, balancer);

        let mut servers = VecDeque::with_capacity(self.servers.len());
        for config in self.servers.drain(..) {
            let server = config.mk_server(router.clone(), buf.clone())?;
            servers.push_back(server);
        }

        Ok(RouterSpawner {
               servers: servers,
               resolver_executor: Some(resolver_exec),
           })
    }
}

pub struct RouterSpawner {
    servers: VecDeque<server::Unbound>,
    resolver_executor: Option<resolver::Executor>,
}

impl RouterSpawner {
    pub fn spawn(mut self, reactor: &Handle) -> Result<(), ConfigError> {
        while let Some(unbound) = self.servers.pop_front() {
            let bound = unbound.bind(&reactor);
        }
        unimplemented!();
    }
}

pub struct AdminRunner {
    metrics: tacho::Reporter,
    resolvers: VecDeque<resolver::Executor>,
}

impl AdminRunner {
    pub fn run(mut self,
               closer: oneshot::Sender<()>,
               mut reactor: Core)
               -> Result<(), ConfigError> {
        let handle = reactor.handle();
        while let Some(r) = self.resolvers.pop_front() {
            let e = r.execute(&handle);
            handle.spawn(e);
        }
        let _ = closer.send(());
        reactor.run(future::ok(()))
    }
}
