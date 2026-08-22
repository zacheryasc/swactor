use std::fs;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PressureSample {
    pub some_avg10: f64,
    pub some_avg60: f64,
    pub some_avg300: f64,
    pub some_total_us: u64,
    pub full_avg10: Option<f64>,
    pub full_avg60: Option<f64>,
    pub full_avg300: Option<f64>,
    pub full_total_us: Option<u64>,
}

pub(crate) fn read(resource: &str) -> Result<PressureSample, String> {
    let path = format!("/proc/pressure/{resource}");
    let raw = fs::read_to_string(&path).map_err(|error| format!("read {path}: {error}"))?;
    parse(&raw).ok_or_else(|| format!("parse {path}"))
}

fn parse(raw: &str) -> Option<PressureSample> {
    let some = parse_row(raw.lines().find(|line| line.starts_with("some "))?)?;
    let full = raw
        .lines()
        .find(|line| line.starts_with("full "))
        .and_then(parse_row);
    Some(PressureSample {
        some_avg10: some.avg10,
        some_avg60: some.avg60,
        some_avg300: some.avg300,
        some_total_us: some.total_us,
        full_avg10: full.as_ref().map(|row| row.avg10),
        full_avg60: full.as_ref().map(|row| row.avg60),
        full_avg300: full.as_ref().map(|row| row.avg300),
        full_total_us: full.map(|row| row.total_us),
    })
}

#[derive(Debug, Clone, Copy)]
struct PressureRow {
    avg10: f64,
    avg60: f64,
    avg300: f64,
    total_us: u64,
}

fn parse_row(line: &str) -> Option<PressureRow> {
    let mut avg10 = None;
    let mut avg60 = None;
    let mut avg300 = None;
    let mut total_us = None;
    for field in line.split_whitespace().skip(1) {
        let (name, value) = field.split_once('=')?;
        match name {
            "avg10" => avg10 = value.parse().ok(),
            "avg60" => avg60 = value.parse().ok(),
            "avg300" => avg300 = value.parse().ok(),
            "total" => total_us = value.parse().ok(),
            _ => {}
        }
    }
    Some(PressureRow {
        avg10: avg10?,
        avg60: avg60?,
        avg300: avg300?,
        total_us: total_us?,
    })
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn parses_some_and_full_pressure_rows() {
        let sample = parse(
            "some avg10=1.25 avg60=2.50 avg300=3.75 total=1234\nfull avg10=0.10 avg60=0.20 avg300=0.30 total=42\n",
        )
        .expect("pressure sample");
        assert_eq!(sample.some_avg10, 1.25);
        assert_eq!(sample.some_total_us, 1234);
        assert_eq!(sample.full_avg10, Some(0.10));
        assert_eq!(sample.full_total_us, Some(42));
    }

    #[test]
    fn accepts_cpu_pressure_without_full_row() {
        let sample =
            parse("some avg10=0.00 avg60=0.01 avg300=0.02 total=99\n").expect("pressure sample");
        assert_eq!(sample.some_total_us, 99);
        assert_eq!(sample.full_avg10, None);
    }
}
