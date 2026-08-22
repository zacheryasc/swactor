use std::collections::HashSet;

use crate::types::{Offer, SelectionPolicy};

pub(crate) fn reachable_offers(offers: Vec<Offer>, policy: &SelectionPolicy) -> Vec<Offer> {
    let blacklist: HashSet<u64> = policy.blacklist_hosts.iter().copied().collect();
    offers
        .into_iter()
        .filter(|o| {
            o.geolocation
                .as_deref()
                .is_some_and(|geolocation| !geolocation.to_uppercase().contains("CN"))
        })
        .filter(|offer| offer.host_id.is_none_or(|host| !blacklist.contains(&host)))
        .filter(|o| o.verification.as_deref() != Some("deverified"))
        .filter(|o| policy.max_dph_total.is_none_or(|max| o.dph_total <= max))
        .filter(|o| {
            policy
                .min_compute_cap
                .is_none_or(|min_compute_cap| o.compute_cap >= min_compute_cap)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offer(id: u64, dph_total: f64) -> Offer {
        Offer {
            id,
            gpu_name: "Tesla T4".to_owned(),
            dph_total,
            gpu_ram: Some(16_000.0),
            compute_cap: 750,
            geolocation: Some("US".to_owned()),
            inet_down_cost_per_tb: 0.0,
            inet_up_cost_per_tb: 0.0,
            host_id: Some(id),
            verification: Some("unverified".to_owned()),
            reliability2: Some(0.99),
            inet_down: Some(500.0),
            inet_up: Some(250.0),
        }
    }

    #[test]
    fn max_price_cap_keeps_only_affordable_reachable_offers() {
        let policy = SelectionPolicy {
            max_dph_total: Some(0.10),
            ..SelectionPolicy::default()
        };

        let reachable = reachable_offers(vec![offer(1, 0.09), offer(2, 0.11)], &policy);

        assert_eq!(reachable.len(), 1);
        assert_eq!(reachable[0].id, 1);
    }

    #[test]
    fn minimum_compute_cap_keeps_only_modern_cuda_offers() {
        let mut legacy = offer(1, 0.03);
        legacy.compute_cap = 520;
        let mut modern = offer(2, 0.04);
        modern.compute_cap = 750;

        let reachable = reachable_offers(vec![legacy, modern], &SelectionPolicy::default());

        assert_eq!(reachable.len(), 1);
        assert_eq!(reachable[0].id, 2);
    }
}
