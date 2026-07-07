use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::record::Record;
use serde::{Deserialize, Serialize};

pub const HOST_NET_CHANNEL: &str = "host.net";
pub const HOST_NET_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

const SCHEMA: &str = "host.net.v1";
const PROC_NET_DEV: &str = "/proc/net/dev";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostNetSample {
    pub schema: String,
    pub seq: u64,
    pub sample_unix_ms: u64,
    pub interfaces: Vec<NetInterfaceSample>,
    pub error: Option<String>,
}

impl HostNetSample {
    pub fn error(seq: u64, error: impl Into<String>) -> Self {
        Self {
            schema: SCHEMA.to_string(),
            seq,
            sample_unix_ms: unix_ms_now(),
            interfaces: Vec::new(),
            error: Some(error.into()),
        }
    }
}

impl Record for HostNetSample {
    const CHANNEL: &'static str = HOST_NET_CHANNEL;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetInterfaceSample {
    pub name: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_packets: u64,
    pub tx_packets: u64,
    pub rx_errors: u64,
    pub tx_errors: u64,
    pub rx_dropped: u64,
    pub tx_dropped: u64,
}

pub fn sample(seq: u64) -> HostNetSample {
    let sample_unix_ms = unix_ms_now();
    match fs::read_to_string(PROC_NET_DEV) {
        Ok(raw) => HostNetSample {
            schema: SCHEMA.to_string(),
            seq,
            sample_unix_ms,
            interfaces: parse_proc_net_dev(&raw),
            error: None,
        },
        Err(error) => HostNetSample {
            schema: SCHEMA.to_string(),
            seq,
            sample_unix_ms,
            interfaces: Vec::new(),
            error: Some(format!("read {PROC_NET_DEV}: {error}")),
        },
    }
}

fn parse_proc_net_dev(raw: &str) -> Vec<NetInterfaceSample> {
    raw.lines().filter_map(parse_interface_line).collect()
}

fn parse_interface_line(line: &str) -> Option<NetInterfaceSample> {
    let (name, counters) = line.split_once(':')?;
    let fields = counters.split_whitespace().collect::<Vec<_>>();
    if fields.len() < 16 {
        return None;
    }

    Some(NetInterfaceSample {
        name: name.trim().to_string(),
        rx_bytes: parse_u64(fields[0]),
        rx_packets: parse_u64(fields[1]),
        rx_errors: parse_u64(fields[2]),
        rx_dropped: parse_u64(fields[3]),
        tx_bytes: parse_u64(fields[8]),
        tx_packets: parse_u64(fields[9]),
        tx_errors: parse_u64(fields[10]),
        tx_dropped: parse_u64(fields[11]),
    })
}

fn parse_u64(value: &str) -> u64 {
    value.parse::<u64>().unwrap_or(0)
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_proc_net_dev_interfaces_and_ignores_non_interface_lines() {
        let interfaces = parse_proc_net_dev(
            "Inter-|   Receive                                                |  Transmit\n\
             face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n\
               lo: 100 2 0 0 0 0 0 0 200 3 0 0 0 0 0 0\n\
             enp0s3: 123456 789 1 2 0 0 0 0 654321 987 3 4 0 0 0 0\n\
             malformed: 1 2 3\n",
        );

        assert_eq!(
            interfaces,
            vec![
                NetInterfaceSample {
                    name: "lo".to_string(),
                    rx_bytes: 100,
                    tx_bytes: 200,
                    rx_packets: 2,
                    tx_packets: 3,
                    rx_errors: 0,
                    tx_errors: 0,
                    rx_dropped: 0,
                    tx_dropped: 0,
                },
                NetInterfaceSample {
                    name: "enp0s3".to_string(),
                    rx_bytes: 123456,
                    tx_bytes: 654321,
                    rx_packets: 789,
                    tx_packets: 987,
                    rx_errors: 1,
                    tx_errors: 3,
                    rx_dropped: 2,
                    tx_dropped: 4,
                },
            ]
        );
    }

    #[test]
    fn malformed_numeric_counters_parse_to_zero() {
        let interfaces =
            parse_proc_net_dev("eth0: nope 10 bad 12 0 0 0 0 missing 20 bad_tx 22 0 0 0 0\n");

        assert_eq!(interfaces.len(), 1);
        assert_eq!(interfaces[0].rx_bytes, 0);
        assert_eq!(interfaces[0].rx_packets, 10);
        assert_eq!(interfaces[0].rx_errors, 0);
        assert_eq!(interfaces[0].rx_dropped, 12);
        assert_eq!(interfaces[0].tx_bytes, 0);
        assert_eq!(interfaces[0].tx_packets, 20);
        assert_eq!(interfaces[0].tx_errors, 0);
        assert_eq!(interfaces[0].tx_dropped, 22);
    }

    #[test]
    fn sample_error_uses_host_net_channel_schema_seq_and_error() {
        let sample = HostNetSample::error(11, "no net dev");

        assert_eq!(HostNetSample::CHANNEL, "host.net");
        assert_eq!(sample.schema, "host.net.v1");
        assert_eq!(sample.seq, 11);
        assert_eq!(sample.interfaces, Vec::<NetInterfaceSample>::new());
        assert_eq!(sample.error, Some("no net dev".to_string()));
    }
}
