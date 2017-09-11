use super::config::TlsServerIdentityConfig;
use rustls::{Certificate, ResolvesServerCert, SignatureScheme, sign};
use rustls::internal::pemfile;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::sync::Arc;

pub fn new(
    identities: &Option<HashMap<String, TlsServerIdentityConfig>>,
    default: &Option<TlsServerIdentityConfig>,
) -> Result<Sni, Error> {
    let n_identities = identities.as_ref().map(|ids| ids.len()).unwrap_or(0);
    let default = match default {
        &Some(ref c) => Some(ServerIdentity::load(c)?),
        &None if n_identities > 0 => return Err(Error::NoIdentities),
        &None => None,
    };
    let sni = Sni {
        default,
        identities: {
            let mut ids = HashMap::with_capacity(n_identities);
            if let Some(identities) = identities.as_ref() {
                for (k, c) in identities {
                    let k: String = (*k).clone();
                    let v = ServerIdentity::load(c)?;
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
    FailedToOpenCertificateFile(String, io::Error),
    FailedToReadCertificateFile(String),
    FailedToOpenPrivateKeyFile(String, io::Error),
    FailedToReadPrivateKeyFile(String),
    WrongNumberOfKeysInPrivateKeyFile(String, usize),
    FailedToConstructPrivateKey(String),
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
    fn load(c: &TlsServerIdentityConfig) -> Result<ServerIdentity, Error> {
        let mut certs = vec![];
        for p in &c.certs {
            certs.append(&mut load_certs(p)?);
        }
        let key = load_private_key(&c.private_key)?;
        Ok(ServerIdentity { key: sign::CertifiedKey::new(certs, Arc::new(key)) })
    }
}

// from rustls example
fn load_certs(cert_file_path: &String) -> Result<Vec<Certificate>, Error> {
    let file = File::open(&cert_file_path)
        .map_err(|e| Error::FailedToOpenCertificateFile(cert_file_path.clone(), e))?;
    let mut r = io::BufReader::new(file);
    pemfile::certs(&mut r)
        .map_err(|()| Error::FailedToReadCertificateFile(cert_file_path.clone()))
}

// from rustls example
fn load_private_key(key_file_path: &String) -> Result<Box<sign::SigningKey>, Error> {
    let file = File::open(&key_file_path)
        .map_err(|e| Error::FailedToOpenPrivateKeyFile(key_file_path.clone(), e))?;
    let mut r = io::BufReader::new(file);
    let keys = pemfile::rsa_private_keys(&mut r)
        .map_err(|()| Error::FailedToReadPrivateKeyFile(key_file_path.clone()))?;
    if keys.len() != 1 {
        return Err(Error::WrongNumberOfKeysInPrivateKeyFile(key_file_path.clone(), keys.len()));
    }
    let key = sign::RSASigningKey::new(&keys[0])
        .map_err(|()| Error::FailedToConstructPrivateKey(key_file_path.clone()))?;
    Ok(Box::new(key))
}
