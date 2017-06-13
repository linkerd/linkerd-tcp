use super::{Connector, ConnectorFactory, Tls};
use super::super::ConfigError;
use rustls;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::time;

const DEFAULT_MAX_WAITERS: usize = 1_000_000;
const DEFAULT_MAX_CONSECUTIVE_FAILURES: usize = 5;
const DEFAULT_FAILURE_PENALTY_SECS: u64 = 60;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum ConnectorFactoryConfig {
    #[serde(rename = "io.l5d.global")]
    Global(ConnectorConfig),

    #[serde(rename = "io.l5d.static")]
    Static { configs: Vec<ConnectorConfig> },
}

impl Default for ConnectorFactoryConfig {
    fn default() -> ConnectorFactoryConfig {
        ConnectorFactoryConfig::Global(ConnectorConfig::default())
    }
}

impl ConnectorFactoryConfig {
    pub fn mk_connector_factory(&self) -> Result<ConnectorFactory, ConfigError> {
        match *self {
            ConnectorFactoryConfig::Global(ref cfg) => {
                if cfg.prefix.is_some() {
                    return Err("`prefix` not supported in io.l5d.global".into());
                }
                let conn = cfg.mk_connector()?;
                Ok(ConnectorFactory::new_global(conn))
            }
            ConnectorFactoryConfig::Static { ref configs } => {
                let mut pfx_configs = Vec::with_capacity(configs.len());
                for cfg in configs {
                    match cfg.prefix {
                        None => {
                            return Err("`prefix` required in io.l5d.static".into());
                        }
                        Some(ref pfx) => {
                            pfx_configs.push((pfx.clone().into(), cfg.clone()));
                        } 
                    }
                }
                Ok(ConnectorFactory::new_prefixed(pfx_configs))
            }
        }
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ConnectorConfig {
    pub prefix: Option<String>,
    pub tls: Option<TlsConnectorFactoryConfig>,
    pub connect_timeout_ms: Option<u64>,

    pub max_waiters: Option<usize>,
    pub min_connections: Option<usize>,

    pub fail_fast: Option<FailFastConfig>,
    
    // TODO requeue_budget: Option<RequeueBudget>
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FailFastConfig {
    pub max_consecutive_failures: Option<usize>,
    pub failure_penalty_secs: Option<u64>,
}

impl ConnectorConfig {
    pub fn mk_connector(&self) -> Result<Connector, ConfigError> {
        let tls = match self.tls {
            None => None,
            Some(ref tls) => Some(tls.mk_tls()?),
        };
        let connect_timeout = self.connect_timeout_ms.map(time::Duration::from_millis);
        let max_waiters = self.max_waiters.unwrap_or(DEFAULT_MAX_WAITERS);
        let min_conns = self.min_connections.unwrap_or(0);
        let max_fails = self.fail_fast
            .as_ref()
            .and_then(|c| c.max_consecutive_failures)
            .unwrap_or(DEFAULT_MAX_CONSECUTIVE_FAILURES);
        let fail_penalty = {
            let s = self.fail_fast
                .as_ref()
                .and_then(|c| c.failure_penalty_secs)
                .unwrap_or(DEFAULT_FAILURE_PENALTY_SECS);
            time::Duration::from_secs(s)
        };
        Ok(super::new(connect_timeout,
                      tls,
                      max_waiters,
                      min_conns,
                      max_fails,
                      fail_penalty))
    }

    pub fn update(&mut self, other: &ConnectorConfig) {
        if let Some(ref otls) = other.tls {
            self.tls = Some(otls.clone());
        }
        if let Some(ct) = other.connect_timeout_ms {
            self.connect_timeout_ms = Some(ct);
        }
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TlsConnectorFactoryConfig {
    pub dns_name: String,
    pub trust_certs: Option<Vec<String>>,
}

impl TlsConnectorFactoryConfig {
    pub fn mk_tls(&self) -> Result<Tls, ConfigError> {
        let mut config = rustls::ClientConfig::new();
        if let Some(ref certs) = self.trust_certs {
            for p in certs {
                let f = File::open(p).expect("cannot open certificate file");
                config
                    .root_store
                    .add_pem_file(&mut BufReader::new(f))
                    .expect("certificate error");
            }
        };
        let tls = Tls {
            name: self.dns_name.clone(),
            config: Arc::new(config),
        };
        Ok(tls)
    }
}
