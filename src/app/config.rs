

use lb::WithAddr;
use serde_json;
use serde_yaml;
use std::{io, net};
use std::collections::HashMap;

pub fn from_str(mut txt: &str) -> io::Result<AppConfig> {
    txt = txt.trim_left();
    if txt.starts_with('{') {
        serde_json::from_str(txt).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    } else {
        serde_yaml::from_str(txt).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AppConfig {
    pub admin: Option<AdminConfig>,
    pub metrics_interval_secs: Option<u64>,
    pub proxies: Vec<ProxyConfig>,
    pub buffer_size: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AdminConfig {
    pub addr: Option<net::SocketAddr>,
    pub metrics_interval_secs: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProxyConfig {
    pub label: String,
    pub servers: Vec<ServerConfig>,
    pub namerd: NamerdConfig,
    pub client: Option<ClientConfig>,
    pub max_waiters: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum ServerConfig {
    #[serde(rename = "io.l5d.tcp")]
    Tcp { addr: net::SocketAddr },

    // TODO support cypher suites
    // TODO support client auth
    // TODO supoprt persistence?
    #[serde(rename = "io.l5d.tls", rename_all = "camelCase")]
    Tls {
        addr: net::SocketAddr,
        alpn_protocols: Option<Vec<String>>,
        default_identity: Option<TlsServerIdentity>,
        identities: Option<HashMap<String, TlsServerIdentity>>,
    },
}

impl WithAddr for ServerConfig {
    fn addr(&self) -> net::SocketAddr {
        match *self {
            ServerConfig::Tcp { ref addr } |
            ServerConfig::Tls { ref addr, .. } => *addr,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TlsServerIdentity {
    pub certs: Vec<String>,
    pub private_key: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NamerdConfig {
    pub url: String,
    pub path: String,
    pub namespace: Option<String>,
    pub interval_secs: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ClientConfig {
    pub tls: Option<TlsClientConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TlsClientConfig {
    pub dns_name: String,
    pub trust_certs: Option<Vec<String>>,
}

#[test]
fn parse_simple_yaml() {
    let yaml = "
bufferSize: 1234
proxies:
  - label: default
    servers:
      - kind: io.l5d.tcp
        addr: 0.0.0.0:4321
      - kind: io.l5d.tcp
        addr: 0.0.0.0:4322
    namerd:
      url: http://127.0.0.1:4180
      path: /svc/default
      intervalSecs: 5
";
    let app = from_str(yaml).unwrap();
    assert!(app.buffer_size == Some(1234));
    assert!(app.proxies.len() == 1);
}

#[test]
fn parse_simple_json() {
    let json = "{\"bufferSize\":1234, \"proxies\": [{\"label\": \"default\",\
                 \"servers\": [{\"kind\":\"io.l5d.tcp\", \"addr\":\"0.0.0.0:4321\"},\
                 {\"kind\":\"io.l5d.tcp\", \"addr\":\"0.0.0.0:4322\"}],\
                 \"namerd\": {\"url\":\"http://127.0.0.1:4180\", \"path\":\"/svc/default\"}}]}";
    let app = from_str(json).unwrap();
    assert!(app.buffer_size == Some(1234));
    assert!(app.proxies.len() == 1);
}
