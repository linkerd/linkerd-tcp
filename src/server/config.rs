use super::{Unbound, sni};
use super::super::router::Router;
use rustls;
use std::cell::RefCell;
use std::collections::HashMap;
use std::net;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tacho;

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    NoDstName,
    Sni(sni::Error),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ServerConfig {
    port: u16,
    ip: Option<net::IpAddr>,
    dst_name: Option<String>,
    tls: Option<TlsServerConfig>,
    connect_timeout_ms: Option<u64>,
    connection_lifetime_secs: Option<u64>,
    max_concurrency: Option<usize>,
    // TODO idle time
}

impl ServerConfig {
    pub fn mk_server(
        &self,
        router: Router,
        buf: Rc<RefCell<Vec<u8>>>,
        metrics: &tacho::Scope,
    ) -> Result<Unbound> {
        match *self {
            ServerConfig {
                port,
                ref ip,
                ref dst_name,
                ref tls,
                ref connect_timeout_ms,
                ref connection_lifetime_secs,
                ref max_concurrency,
            } => {
                if dst_name.is_none() {
                    return Err(Error::NoDstName);
                }
                let dst_name = dst_name.as_ref().unwrap().clone();
                let ip = ip.unwrap_or_else(|| net::IpAddr::V4(net::Ipv4Addr::new(127, 0, 0, 1)));
                let addr = net::SocketAddr::new(ip, port);
                let tls = match tls.as_ref() {
                    None => None,
                    Some(&TlsServerConfig {
                             ref alpn_protocols,
                             ref default_identity,
                             ref identities,
                         }) => {
                        let mut tls = rustls::ServerConfig::new();
                        if let Some(protos) = alpn_protocols.as_ref() {
                            tls.set_protocols(protos);
                        }
                        let sni = sni::new(identities, default_identity).map_err(Error::Sni)?;
                        tls.cert_resolver = Arc::new(sni);
                        Some(super::UnboundTls { config: Arc::new(tls) })
                    }
                };
                let timeout = connect_timeout_ms.map(Duration::from_millis);
                let lifetime = connection_lifetime_secs.map(Duration::from_secs);
                let max_concurrency = max_concurrency.unwrap_or(super::DEFAULT_MAX_CONCURRENCY);
                Ok(super::unbound(
                    addr,
                    dst_name.into(),
                    router,
                    buf,
                    tls,
                    timeout,
                    lifetime,
                    max_concurrency,
                    metrics,
                ))
            }
        }
    }
}

// TODO support cypher suites
// TODO support client validation
// TODO supoprt persistence?
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TlsServerConfig {
    pub alpn_protocols: Option<Vec<String>>,
    pub default_identity: Option<TlsServerIdentityConfig>,
    pub identities: Option<HashMap<String, TlsServerIdentityConfig>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TlsServerIdentityConfig {
    pub certs: Vec<String>,
    pub private_key: String,
}
