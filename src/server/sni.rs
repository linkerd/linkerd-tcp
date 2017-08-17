use super::config::TlsServerIdentityConfig;
use rustls::{Certificate, ResolvesServerCert, SignatureScheme, sign};
use rustls::internal::pemfile;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

pub fn new(
    identities: &Option<HashMap<String, TlsServerIdentityConfig>>,
    default: &Option<TlsServerIdentityConfig>,
) -> Result<Sni, Error> {
    let n_identities = identities.as_ref().map(|ids| ids.len()).unwrap_or(0);
    if default.is_none() && n_identities > 0 {
        return Err(Error::NoIdentities);
    }
    let sni = Sni {
        default: default.as_ref().map(|c| ServerIdentity::load(c)),
        identities: {
            let mut ids = HashMap::with_capacity(n_identities);
            if let Some(identities) = identities.as_ref() {
                for (k, c) in identities {
                    let k: String = (*k).clone();
                    let v = ServerIdentity::load(c);
                    ids.insert(k, v);
                }
            }
            Arc::new(ids)
        },
    };
    Ok(sni)
}

#[derive(Debug)]
pub enum Error {
    NoIdentities,
}

pub struct Sni {
    default: Option<ServerIdentity>,
    identities: Arc<HashMap<String, ServerIdentity>>,
}

impl ResolvesServerCert for Sni {
    fn resolve(
        &self,
        server_name: Option<&str>,
        _sigschemes: &[SignatureScheme],
    ) -> Option<sign::CertifiedKey> {
        debug!("finding cert resolver for {:?}", server_name);
        server_name
            .and_then(|n| {
                debug!("found match for {}", n);
                self.identities.get(n)
            })
            .or_else(|| {
                debug!("reverting to default");
                self.default.as_ref()
            })
            .map(|id| id.key.clone())
    }
}

struct ServerIdentity {
    key: sign::CertifiedKey,
}

impl ServerIdentity {
    fn load(c: &TlsServerIdentityConfig) -> ServerIdentity {
        let mut certs = vec![];
        for p in &c.certs {
            certs.append(&mut load_certs(p));
        }
        let key = Arc::new(load_private_key(&c.private_key));
        ServerIdentity {
            key: sign::CertifiedKey::new(certs, key),
        }
    }
}

// from rustls example
fn load_certs(filename: &str) -> Vec<Certificate> {
    let certfile = File::open(filename).expect("cannot open certificate file");
    let mut r = BufReader::new(certfile);
    pemfile::certs(&mut r).unwrap()
}

// from rustls example
fn load_private_key(filename: &str) -> Box<sign::SigningKey> {
    let keyfile = File::open(filename).expect("cannot open private key file");
    let mut r = BufReader::new(keyfile);
    let keys = pemfile::rsa_private_keys(&mut r).unwrap();
    assert_eq!(keys.len(), 1);
    Box::new(sign::RSASigningKey::new(&keys[0]).expect(
        "Invalid RSA private key",
    ))
}
