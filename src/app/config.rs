use serde_json;
use serde_yaml;
use std::{io, net, time};

pub fn from_str(mut txt: &str) -> io::Result<AppConfig> {
    txt = txt.trim_left();
    if txt.starts_with('{') {
        serde_json::from_str(txt).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    } else {
        serde_yaml::from_str(txt).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppConfig {
    pub proxies: Vec<ProxyConfig>,
    pub buffer_size: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ProxyConfig {
    pub servers: Vec<ServerConfig>,
    pub namerd: NamerdConfig,
    pub client: Option<ClientConfig>,
    pub max_waiters: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ServerConfig {
    pub addr: net::SocketAddr,
    pub tls: Option<TlsServerConfig>,
}

// TODO support cypher suites
// TODO support SNI
// TODO support client auth
// TODO supoprt persistence?
#[derive(Serialize, Deserialize, Debug)]
pub struct TlsServerConfig {
    pub alpn_protocols: Option<Vec<String>>,
    pub cert_paths: Vec<String>,
    pub private_key_path: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NamerdConfig {
    pub addr: net::SocketAddr,
    pub path: String,
    pub namespace: Option<String>,
    pub interval: Option<time::Duration>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientConfig {
    pub tls: Option<TlsClientConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TlsClientConfig {
    pub name: String,
    pub trust_cert_paths: Option<Vec<String>>,
}

#[test]
fn parse_simple_yaml() {
    let yaml = "
buffer_size: 8192
proxies:
  - servers:
      - addr: 0.0.0.0:4321
      - addr: 0.0.0.0:4322
    namerd:
      addr: 127.0.0.1:4180
      path: /svc/default
";
    let app = from_str(yaml).unwrap();
    assert!(app.proxies.len() == 1);
}

#[test]
fn parse_simple_json() {
    let json = "{\"buffer_size\": 8192, \"proxies\": [{\"servers\": [\
                  {\"addr\": \"0.0.0.0:4321\"},{\"addr\": \"0.0.0.0:4322\"}],\
                  \"namerd\": {\"addr\": \"127.0.0.1:4180\", \"path\": \"/svc/default\"}}]}";
    let app = from_str(json).unwrap();
    assert!(app.proxies.len() == 1);
}
