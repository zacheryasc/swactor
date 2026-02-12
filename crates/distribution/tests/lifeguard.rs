//! Behavioral tests for Lifeguard protocol extensions.
//!
//! Tests verify the three Lifeguard mechanisms from the consumer's perspective:
//! 1. Local Health Multiplier (LHM) — degraded nodes get stretched timeouts
//! 2. Dynamic suspect timeout — scales with cluster size
//! 3. Protocol period scaling — probe intervals stretch under load

use distribution::swim::lifeguard::{HealthMultiplier, LifeguardConfig};

// ─── Local Health Multiplier ─────────────────────────────────────────────────

#[test]
fn healthy_node_has_multiplier_of_one() {
    // Given: a freshly created health multiplier
    let hm = HealthMultiplier::new(LifeguardConfig::default());

    // Then: multiplier is 1 (no scaling)
    assert_eq!(hm.multiplier(), 1);
    assert_eq!(hm.score(), 0);
}

#[test]
fn nacks_degrade_health_and_increase_multiplier() {
    // Given: a healthy node
    let mut hm = HealthMultiplier::new(LifeguardConfig::default());

    // When: 3 consecutive nacks occur (no acks)
    hm.record_nack();
    hm.record_nack();
    hm.record_nack();

    // Then: health score is 3, multiplier is 4
    assert_eq!(hm.score(), 3);
    assert_eq!(hm.multiplier(), 4);
}

#[test]
fn acks_improve_health() {
    // Given: a degraded node (score = 3)
    let mut hm = HealthMultiplier::new(LifeguardConfig::default());
    hm.record_nack();
    hm.record_nack();
    hm.record_nack();

    // When: 2 successful acks arrive
    hm.record_ack();
    hm.record_ack();

    // Then: health improves
    assert_eq!(hm.score(), 1);
    assert_eq!(hm.multiplier(), 2);
}

#[test]
fn health_score_cannot_go_below_zero() {
    // Given: a healthy node
    let mut hm = HealthMultiplier::new(LifeguardConfig::default());

    // When: acks arrive despite no prior nacks
    hm.record_ack();
    hm.record_ack();
    hm.record_ack();

    // Then: score stays at 0
    assert_eq!(hm.score(), 0);
    assert_eq!(hm.multiplier(), 1);
}

#[test]
fn health_score_capped_at_max() {
    // Given: a config with max_health_score = 4
    let config = LifeguardConfig {
        max_health_score: 4,
        ..LifeguardConfig::default()
    };
    let mut hm = HealthMultiplier::new(config);

    // When: many nacks occur
    for _ in 0..20 {
        hm.record_nack();
    }

    // Then: score is capped at 4, multiplier at 5
    assert_eq!(hm.score(), 4);
    assert_eq!(hm.multiplier(), 5);
}

// ─── Protocol Period Scaling ─────────────────────────────────────────────────

#[test]
fn healthy_node_uses_base_probe_interval() {
    // Given: a healthy node
    let hm = HealthMultiplier::new(LifeguardConfig::default());

    // When: computing scaled probe interval with base = 10
    let interval = hm.scaled_probe_interval(10);

    // Then: interval is unchanged (multiplier = 1)
    assert_eq!(interval, 10);
}

#[test]
fn degraded_node_stretches_probe_interval() {
    // Given: a node with health score 3 (multiplier = 4)
    let mut hm = HealthMultiplier::new(LifeguardConfig::default());
    hm.record_nack();
    hm.record_nack();
    hm.record_nack();

    // When: computing scaled probe interval with base = 10
    let interval = hm.scaled_probe_interval(10);

    // Then: interval is stretched to 40
    assert_eq!(interval, 40);
}

#[test]
fn degraded_node_stretches_probe_timeout() {
    // Given: a node with health score 2
    let mut hm = HealthMultiplier::new(LifeguardConfig::default());
    hm.record_nack();
    hm.record_nack();

    // When: computing scaled probe timeout with base = 3
    let timeout = hm.scaled_probe_timeout(3);

    // Then: timeout is stretched to 9 (3 * multiplier 3)
    assert_eq!(timeout, 9);
}

// ─── Dynamic Suspect Timeout ─────────────────────────────────────────────────

#[test]
fn suspect_timeout_scales_with_cluster_size() {
    // Given: a healthy node with base_suspicion_timeout = 30
    let config = LifeguardConfig {
        base_suspicion_timeout: 30,
        min_suspicion_timeout: 10,
        max_suspicion_timeout: 500,
        ..LifeguardConfig::default()
    };
    let hm = HealthMultiplier::new(config);

    // When: computing dynamic timeout for different cluster sizes
    let timeout_2 = hm.dynamic_suspicion_timeout(2);
    let timeout_8 = hm.dynamic_suspicion_timeout(8);
    let timeout_100 = hm.dynamic_suspicion_timeout(100);

    // Then: larger clusters get longer timeouts (log2 scaling)
    assert!(
        timeout_2 < timeout_8,
        "8-node cluster should have longer timeout than 2-node: {} vs {}",
        timeout_2, timeout_8
    );
    assert!(
        timeout_8 < timeout_100,
        "100-node cluster should have longer timeout than 8-node: {} vs {}",
        timeout_8, timeout_100
    );
}

#[test]
fn suspect_timeout_is_clamped_to_min() {
    // Given: a config with min_suspicion_timeout = 50 and a tiny cluster
    let config = LifeguardConfig {
        base_suspicion_timeout: 1,
        min_suspicion_timeout: 50,
        max_suspicion_timeout: 500,
        ..LifeguardConfig::default()
    };
    let hm = HealthMultiplier::new(config);

    // When: computing for a 2-node cluster (log2(3) ≈ 2, so base*2*1 = 2)
    let timeout = hm.dynamic_suspicion_timeout(2);

    // Then: clamped to minimum
    assert_eq!(timeout, 50);
}

#[test]
fn suspect_timeout_is_clamped_to_max() {
    // Given: a config with max_suspicion_timeout = 100 and a huge cluster
    let config = LifeguardConfig {
        base_suspicion_timeout: 30,
        min_suspicion_timeout: 10,
        max_suspicion_timeout: 100,
        ..LifeguardConfig::default()
    };
    let hm = HealthMultiplier::new(config);

    // When: computing for a 10000-node cluster
    let timeout = hm.dynamic_suspicion_timeout(10000);

    // Then: clamped to maximum
    assert_eq!(timeout, 100);
}

#[test]
fn degraded_health_further_increases_suspect_timeout() {
    // Given: a config and two nodes — one healthy, one degraded
    let config = LifeguardConfig {
        base_suspicion_timeout: 30,
        min_suspicion_timeout: 10,
        max_suspicion_timeout: 5000,
        ..LifeguardConfig::default()
    };
    let healthy = HealthMultiplier::new(config.clone());
    let mut degraded = HealthMultiplier::new(config);
    degraded.record_nack();
    degraded.record_nack();

    // When: both compute timeout for a 16-node cluster
    let healthy_timeout = healthy.dynamic_suspicion_timeout(16);
    let degraded_timeout = degraded.dynamic_suspicion_timeout(16);

    // Then: degraded node gives itself even more time
    assert!(
        degraded_timeout > healthy_timeout,
        "degraded node ({}) should have longer suspect timeout than healthy ({})",
        degraded_timeout, healthy_timeout
    );
    // Specifically: healthy = 30 * log2(17) * 1, degraded = 30 * log2(17) * 3
    assert_eq!(degraded_timeout, healthy_timeout * 3);
}

// ─── Stress / Scenario Tests ─────────────────────────────────────────────────

#[test]
fn recovery_from_worst_health_takes_max_acks() {
    // Given: a node at maximum degradation
    let config = LifeguardConfig {
        max_health_score: 8,
        nack_penalty: 1,
        ack_reward: 1,
        ..LifeguardConfig::default()
    };
    let mut hm = HealthMultiplier::new(config);
    for _ in 0..100 {
        hm.record_nack();
    }
    assert_eq!(hm.score(), 8);

    // When: exactly max_health_score acks arrive
    for _ in 0..8 {
        hm.record_ack();
    }

    // Then: fully recovered
    assert_eq!(hm.score(), 0);
    assert_eq!(hm.multiplier(), 1);
}

#[test]
fn mixed_ack_nack_stream_settles_to_moderate_health() {
    // Given: a node receiving alternating acks and nacks (slightly more nacks)
    let config = LifeguardConfig {
        max_health_score: 10,
        nack_penalty: 2,
        ack_reward: 1,
        ..LifeguardConfig::default()
    };
    let mut hm = HealthMultiplier::new(config);

    // When: 100 rounds of alternating nack, ack
    for _ in 0..100 {
        hm.record_nack(); // +2
        hm.record_ack();  // -1
    }

    // Then: score settles near max (nack caps at 10, final ack brings it to 9)
    assert_eq!(hm.score(), 9);
}

#[test]
fn solo_node_gets_minimal_suspect_timeout() {
    // Given: a healthy node in a cluster of size 1
    let config = LifeguardConfig {
        base_suspicion_timeout: 30,
        min_suspicion_timeout: 15,
        max_suspicion_timeout: 500,
        ..LifeguardConfig::default()
    };
    let hm = HealthMultiplier::new(config);

    // When: computing timeout for cluster of 1
    let timeout = hm.dynamic_suspicion_timeout(1);

    // Then: log2(2) = 1, so 30*1*1 = 30 (above min)
    assert_eq!(timeout, 30);
}

#[test]
fn empty_cluster_still_returns_valid_timeout() {
    // Given: edge case — cluster size 0
    let config = LifeguardConfig {
        base_suspicion_timeout: 30,
        min_suspicion_timeout: 15,
        max_suspicion_timeout: 500,
        ..LifeguardConfig::default()
    };
    let hm = HealthMultiplier::new(config);

    // When/Then: doesn't panic and returns clamped value
    let timeout = hm.dynamic_suspicion_timeout(0);
    assert!(timeout >= 15);
}
