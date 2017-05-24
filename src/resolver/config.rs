use super::namerd::Namerd;
use super::super::ConfigError;
use std::time::Duration;
use tacho;
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NamerdConfig {
    pub base_url: String,
    pub period_secs: u64,
    pub namespace: String,
}

impl NamerdConfig {
    pub fn into_namerd(self, metrics: &tacho::Scope) -> Result<Namerd, ConfigError> {
        if self.period_secs == 0 {
            return Err("namerd period must be greater than 0".into());
        }
        let period = Duration::from_secs(self.period_secs);

        if let Err(e) = Url::parse(&self.base_url) {
            return Err(format!("invalid base_url {}: {}", self.base_url, e).into());
        }

        let namerd = Namerd::new(self.base_url, period, self.namespace, metrics.clone());
        Ok(namerd)
    }
}
