/// Result of a single property check.
#[derive(Debug, Clone)]
pub struct PropertyResult {
    pub name: String,
    pub category: String,
    pub passed: bool,
    pub expected: String,
    pub actual: String,
    pub description: String,
}

/// Standard deviation of a slice of f64 values.
pub fn std_dev(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
    variance.sqrt()
}

/// Coefficient of variation (std_dev / mean).
pub fn coeff_of_variation(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    if mean.abs() < 1e-12 {
        return 0.0;
    }
    std_dev(values) / mean
}

/// Chi-squared statistic against a uniform distribution.
pub fn chi_squared_uniform(observed: &[f64]) -> f64 {
    if observed.is_empty() {
        return 0.0;
    }
    let total: f64 = observed.iter().sum();
    let expected = total / observed.len() as f64;
    if expected.abs() < 1e-12 {
        return 0.0;
    }
    observed
        .iter()
        .map(|&o| (o - expected).powi(2) / expected)
        .sum()
}
