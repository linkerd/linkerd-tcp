use super::Unbound;
use super::sni::{self, Sni};
use super::super::ConfigError;
use super::super::router::Router;
use rustls;
use std::cell::RefCell;
use std::collections::HashMap;
use std::net;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum ServerConfig {
    #[serde(rename = "io.l5d.tcp")]
    Tcp {
        addr: net::SocketAddr,
        dst_name: Option<String>,
    },

    // TODO support cypher suites
    // TODO support client validation
    // TODO supoprt persistence?
    #[serde(rename = "io.l5d.tls", rename_all = "camelCase")]
    Tls {
        addr: net::SocketAddr,
        dst_name: Option<String>,
        alpn_protocols: Option<Vec<String>>,
        default_identity: Option<TlsServerIdentityConfig>,
        identities: Option<HashMap<String, TlsServerIdentityConfig>>,
    },
}

impl ServerConfig {
    pub fn mk_server(&self,
                     router: Router,
                     buf: Rc<RefCell<Vec<u8>>>)
                     -> Result<Unbound, ConfigError> {
        match *self {
            ServerConfig::Tcp {
                ref addr,
                ref dst_name,
            } => {
                if dst_name.is_none() {
                    return Err("".into());
                }
                let dst_name = dst_name.as_ref().unwrap().clone();
                Ok(super::unbound(*addr, dst_name.into(), router, buf, None))
            }
            ServerConfig::Tls {
                ref addr,
                ref dst_name,
                //ref alpn_protocols,
                ref default_identity,
                ref identities,
                ..
            } => {
                let tls = {
                    let mut tls = rustls::ServerConfig::new();
                    let sni = sni::new(identities, default_identity)?;
                    // XXX apply SNI, alpn, ...
                    tls.cert_resolver = Box::new(sni);
                    super::Tls { config: Arc::new(tls) }
                };
                let dst_name = dst_name.as_ref().unwrap().clone();
                Ok(super::unbound(*addr, dst_name.into(), router, buf, Some(tls)))
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TlsServerIdentityConfig {
    pub certs: Vec<String>,
    pub private_key: String,
}
