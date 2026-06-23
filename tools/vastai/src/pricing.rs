use crate::types::{Offer, SelectionPolicy};

/// Cost model for ranking offers on true lease cost rather than listed $/hr.
#[derive(Debug, Clone, Default)]
pub struct CostModel {
    /// Deploy-image size in GB; `None` → image pull is not priced in.
    pub image_gb: Option<f64>,
}

impl CostModel {
    pub fn from_policy(policy: &SelectionPolicy) -> Self {
        Self {
            image_gb: policy.image_size_gb,
        }
    }

    /// One-time cost of pulling the deploy image to this offer's host.
    pub fn pull_cost(&self, o: &Offer) -> f64 {
        self.image_gb
            .map_or(0.0, |gb| gb * o.inet_down_cost_per_tb / 1000.0)
    }

    /// Effective price used for ranking.
    pub fn effective_price(&self, o: &Offer) -> f64 {
        o.dph_total + self.pull_cost(o)
    }
}

/// Drop the suspiciously-cheap tail within each GPU model, then merge by price.
pub(crate) fn rank_survivors(offers: Vec<Offer>, cost: &CostModel, drop_frac: f64) -> Vec<Offer> {
    let mut by_model: std::collections::HashMap<String, Vec<Offer>> =
        std::collections::HashMap::new();
    for o in offers {
        by_model.entry(o.gpu_name.clone()).or_default().push(o);
    }
    let price = |o: &Offer| cost.effective_price(o);
    let by_price = |a: &Offer, b: &Offer| {
        price(a)
            .partial_cmp(&price(b))
            .unwrap_or(std::cmp::Ordering::Equal)
    };
    let mut survivors = Vec::new();
    for (_model, mut group) in by_model {
        group.sort_by(&by_price);
        let drop = (drop_frac * group.len() as f64).floor() as usize;
        survivors.extend(group.into_iter().skip(drop));
    }
    survivors.sort_by(&by_price);
    survivors
}

pub(crate) fn plan_picks(pool: &[Offer], num_instances: u32) -> Vec<&Offer> {
    let mut picks = Vec::with_capacity(num_instances as usize);
    let mut used = std::collections::HashSet::new();
    for o in pool {
        if picks.len() == num_instances as usize {
            break;
        }
        if let Some(h) = o.host_id {
            if !used.insert(h) {
                continue;
            }
        }
        picks.push(o);
    }
    picks
}
