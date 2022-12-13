use crate::behaviour::TransferProtocol;
use clap::Parser;
use log::{debug, warn};
use prometheus_client::encoding::text::{encode, Encode};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{linear_buckets, Histogram};
use prometheus_client::registry::Registry;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

#[derive(Default, Debug, Clone, Eq, Hash, PartialEq, Encode)]
pub struct TransferMeasurement {
    protocol: TransferProtocol,
    pub cid: String,
    pub data_size: u64,
}

impl TransferMeasurement {
    pub fn new(protocol: TransferProtocol, cid: String, data_size: u64) -> Self {
        Self {
            protocol,
            cid,
            data_size,
        }
    }
}

#[derive(Debug, Clone, Parser, Serialize, Deserialize)]
pub struct MetricsConfig {
    endpoint: String,
    service: String,
    instance: String,
}

impl MetricsConfig {
    pub fn from_path(path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                return Err(Box::new(e));
            }
        };
        let mut data = String::new();
        match file.read_to_string(&mut data) {
            Ok(f) => f,
            Err(e) => {
                return Err(Box::new(e));
            }
        };
        match serde_json::from_str(&data) {
            Ok(c) => Ok(c),
            Err(e) => Err(Box::new(e)),
        }
    }
}

pub struct Metrics {
    transfer_measurements: Family<TransferMeasurement, Histogram>,
    registry: Registry,
    push_client: reqwest::Client,
    config: MetricsConfig,
}

impl Metrics {
    pub fn new(config: MetricsConfig) -> Self {
        let mut registry = <Registry>::default();

        let transfers = Family::<TransferMeasurement, Histogram>::new_with_constructor(|| {
            // todo: fine-tune this
            Histogram::new(linear_buckets(0.0, 500.0, 240))
        });
        // Register the metric family with the registry.
        registry.register(
            "transfers",
            "transfer times for a given protocol and cid",
            Box::new(transfers.clone()),
        );

        Metrics {
            registry,
            transfer_measurements: transfers,
            push_client: reqwest::Client::new(),
            config,
        }
    }
}

impl Metrics {
    pub(crate) fn registry(&self) -> &Registry {
        &self.registry
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf = vec![];
        encode(&mut buf, self.registry()).unwrap();
        buf
    }

    pub fn observe(&self, protocol: TransferProtocol, cid: String, data_size: u64, value: f64) {
        let m = TransferMeasurement::new(protocol, cid, data_size);
        let metric = self.transfer_measurements.get_or_create(&m);
        metric.observe(value);
    }

    // once we start collecting constant / continuous measurements this can become a Future that polls every so often for new metrics
    pub async fn push(&self) -> Result<(), Box<dyn std::error::Error>> {
        let prom_gateway_uri = format!(
            "{}/metrics/job/{}/instance/{}",
            self.config.endpoint, self.config.service, self.config.instance
        );
        match self
            .push_client
            .post(&prom_gateway_uri)
            .body(self.encode())
            .send()
            .await
        {
            Ok(res) => match res.status() {
                reqwest::StatusCode::OK => {
                    debug!("pushed metrics to gateway");
                }
                _ => {
                    warn!("failed to push metrics to gateway: {:?}", res);
                    let body = res.text().await.unwrap();
                    warn!("error body: {}", body);
                }
            },
            Err(e) => {
                warn!("failed to connect to gateway: {}", e);
            }
        };

        Ok(())
    }
}
