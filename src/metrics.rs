use chrono::{DateTime, Utc};
use influxdb::Client;
use influxdb::InfluxDbWriteable;
use log::warn;

#[derive(InfluxDbWriteable)]
struct Measurement {
    protocol: String,
    peer: String,
    cid: String,
    transfer_time: i32,
    time: DateTime<Utc>,
}

struct Metrics {
    measurements: Vec<Measurement>,
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
    pub fn add(&mut self, measurement: Measurement) {
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
