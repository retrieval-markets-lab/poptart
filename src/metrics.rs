use chrono::{DateTime, Utc};
use influxdb::Client;
use influxdb::InfluxDbWriteable;
use log::warn;

#[derive(InfluxDbWriteable, Default, Clone)]
pub struct TransferMeasurement {
    protocol: String,
    pub cid: String,
    pub transfer_time: i64,
    pub data_size: i64,
    time: DateTime<Utc>,
}

impl TransferMeasurement {
    pub fn new(protocol: String, cid: String) -> Self {
        TransferMeasurement {
            protocol,
            cid,
            time: Utc::now(),
            ..Default::default()
        }
    }
    pub fn increment(&mut self, block_size: i64) {
        self.transfer_time = (Utc::now() - self.time).num_milliseconds();
        self.data_size += block_size;
    }
}

#[derive(Clone)]
pub struct Metrics {
    measurements: Vec<TransferMeasurement>,
    client: Client,
}

impl Metrics {
    pub fn new(influxdb_url: &str, db: &str) -> Self {
        let client = Client::new(influxdb_url, db);
        Metrics {
            client,
            measurements: vec![],
        }
    }
    pub fn add(&mut self, measurement: TransferMeasurement) {
        self.measurements.push(measurement);
    }

    pub async fn flush_measurements(&mut self) {
        for m in self.measurements.drain(0..) {
            let write_result = self.client.query(m.into_query("retrieval")).await;
            if !write_result.is_ok() {
                warn!("write result was not okay");
            }
        }
    }
}
