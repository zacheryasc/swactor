use std::collections::HashSet;

use crate::types::{Offer, SelectionPolicy};

pub(crate) fn reachable_offers(offers: Vec<Offer>, policy: &SelectionPolicy) -> Vec<Offer> {
    let blacklist: HashSet<u64> = policy.blacklist_hosts.iter().copied().collect();
    offers
        .into_iter()
        .filter(|o| {
            o.geolocation
                .as_deref()
                .map_or(false, |g| !g.to_uppercase().contains("CN"))
        })
        .filter(|o| o.host_id.map_or(true, |h| !blacklist.contains(&h)))
        .filter(|o| o.verification.as_deref() != Some("deverified"))
        .collect()
}
