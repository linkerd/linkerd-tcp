use super::namerd::Namerd;
use std::time::Duration;
use tacho;
use url::{self, Url};

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidPeriod(u64),
    InvalidBaseUrl(String, url::ParseError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NamerdConfig {
    pub base_url: String,
    pub period_secs: u64,
    pub namespace: String,
}

impl NamerdConfig {
    pub fn into_namerd(self, metrics: &tacho::Scope) -> Result<Namerd> {
        if self.period_secs == 0 {
            return Err(Error::InvalidPeriod(self.period_secs));
        }
        let period = Duration::from_secs(self.period_secs);

        if let Err(e) = Url::parse(&self.base_url) {
            return Err(Error::InvalidBaseUrl(self.base_url, e));
        }

        let metrics = metrics.clone().prefixed("resolver").labeled(
            "namespace".into(),
            self.namespace.clone(),
        );
        let namerd = Namerd::new(self.base_url, period, self.namespace, metrics);
        Ok(namerd)
    }
}
