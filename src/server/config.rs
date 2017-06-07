use super::{Unbound, sni};
use super::super::ConfigError;
use super::super::router::Router;
use rustls;
use std::cell::RefCell;
use std::collections::HashMap;
use std::net;
use std::rc::Rc;
use std::sync::Arc;
use tacho;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum ServerConfig {
    #[serde(rename = "io.l5d.tcp")]
    Tcp {
        port: u16,
        ip: Option<net::IpAddr>,
        dst_name: Option<String>,
        tls: Option<TlsServerConfig>,
    },
}

impl ServerConfig {
    pub fn mk_server(&self,
                     router: Router,
                     buf: Rc<RefCell<Vec<u8>>>,
                     metrics: &tacho::Scope)
                     -> Result<Unbound, ConfigError> {
        match *self {
            ServerConfig::Tcp {
                port,
                ref ip,
                ref dst_name,
                ref tls,
            } => {
                if dst_name.is_none() {
                    return Err("`dst_name` required".into());
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
                        let sni = sni::new(identities, default_identity)?;
                        tls.cert_resolver = Box::new(sni);
                        Some(super::UnboundTls { config: Arc::new(tls) })
                    }
                };
                Ok(super::unbound(addr, dst_name.into(), router, buf, tls, metrics))
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
