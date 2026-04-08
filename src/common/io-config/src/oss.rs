use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

pub const DEFAULT_REGION: &str = "cn-hangzhou";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Display)]
#[display(
    "OssConfig
    region: {region:?}
    endpoint: {endpoint:?}
    access_key_id: {access_key_id:?}
    access_key_secret: ***
    security_token: ***
    anonymous: {anonymous}
    max_retries: {max_retries}
    connect_timeout_ms: {connect_timeout_ms}
    read_timeout_ms: {read_timeout_ms}
    max_concurrent_requests: {max_concurrent_requests}
    max_connections_per_io_thread: {max_connections_per_io_thread}"
)]
pub struct OssConfig {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub access_key_secret: Option<ObfuscatedString>,
    pub security_token: Option<ObfuscatedString>,
    pub anonymous: bool,
    pub max_retries: u32,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub max_concurrent_requests: u32,
    pub max_connections_per_io_thread: u32,
    pub multipart_size: u64,
    pub multipart_max_concurrency: u32,
}

impl Default for OssConfig {
    fn default() -> Self {
        Self {
            region: None,
            endpoint: None,
            access_key_id: None,
            access_key_secret: None,
            security_token: None,
            anonymous: false,
            max_retries: 3,
            connect_timeout_ms: 10_000,
            read_timeout_ms: 30_000,
            max_concurrent_requests: 50,
            max_connections_per_io_thread: 50,
            multipart_size: 8 * 1024 * 1024,
            multipart_max_concurrency: 16,
        }
    }
}

impl OssConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(region) = &self.region {
            res.push(format!("Region = {region}"));
        }
        if let Some(endpoint) = &self.endpoint {
            res.push(format!("Endpoint = {endpoint}"));
        }
        if let Some(access_key_id) = &self.access_key_id {
            res.push(format!("Access key id = {access_key_id}"));
        }
        if self.access_key_secret.is_some() {
            res.push("Access key secret = ***".to_string());
        }
        if self.security_token.is_some() {
            res.push("Security token = ***".to_string());
        }
        res.push(format!("Anonymous = {}", self.anonymous));
        res.push(format!("Max retries = {}", self.max_retries));
        res.push(format!("Connect timeout = {}ms", self.connect_timeout_ms));
        res.push(format!("Read timeout = {}ms", self.read_timeout_ms));
        res.push(format!(
            "Max concurrent requests = {}",
            self.max_concurrent_requests
        ));
        res.push(format!(
            "Max connections = {}",
            self.max_connections_per_io_thread
        ));
        res.push(format!("Multipart size = {}", self.multipart_size));
        res.push(format!(
            "Multipart max concurrency = {}",
            self.multipart_max_concurrency
        ));
        res
    }

    pub fn endpoint_and_region(&self) -> (String, String) {
        match (self.endpoint.clone(), self.region.clone()) {
            (Some(ep), Some(re)) => (ep, re),
            (Some(ep), None) => {
                let region = extract_region(&ep).unwrap_or_else(|| {
                    log::warn!(
                        "Cannot extract region from endpoint {ep}, use default region {DEFAULT_REGION}"
                    );
                    DEFAULT_REGION.to_string()
                });
                (ep, region)
            }
            (None, Some(re)) => {
                log::warn!(
                    "Endpoint is not set but found region {re}, use default endpoint oss-{re}.aliyuncs.com"
                );
                (format!("oss-{re}.aliyuncs.com"), re)
            }
            (None, None) => {
                log::warn!(
                    "Both endpoint and region are not found, use default endpoint oss-{DEFAULT_REGION}.aliyuncs.com"
                );
                (
                    format!("oss-{DEFAULT_REGION}.aliyuncs.com"),
                    DEFAULT_REGION.to_string(),
                )
            }
        }
    }
}

pub fn extract_region(endpoint: &str) -> Option<String> {
    let host = endpoint
        .trim_start_matches(|c: char| !c.is_ascii_alphanumeric())
        .trim_start_matches("://")
        .split('/')
        .next()?;

    for part in host.split('.') {
        if let Some(region) = part.strip_prefix("oss-") {
            return Some(region.to_string());
        }
    }

    None
}
