pub use swactor_vastai::SelectionPolicy;
use swactor_vastai::{Offer, VastClient};

#[derive(Clone, Debug, PartialEq)]
pub struct OfferPreview {
    pub offer_id: u64,
    pub host_id: Option<u64>,
    pub gpu_name: String,
    pub gpu_ram_mb: Option<u64>,
    pub dollars_per_hour: f64,
}

impl OfferPreview {
    pub fn from_offer(offer: Offer) -> Self {
        Self {
            offer_id: offer.id,
            host_id: offer.host_id,
            gpu_name: offer.gpu_name,
            gpu_ram_mb: offer.gpu_ram.map(|gb| (gb * 1024.0).round() as u64),
            dollars_per_hour: offer.dph_total,
        }
    }
}

pub trait OfferPreviewer {
    fn preview(&self, api_key: &str, policy: &SelectionPolicy) -> Result<OfferPreview, String>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct VastAiOfferPreviewer;

impl OfferPreviewer for VastAiOfferPreviewer {
    fn preview(&self, api_key: &str, policy: &SelectionPolicy) -> Result<OfferPreview, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("vastai offer preview runtime: {e}"))?;
        let client = VastClient::new(api_key.to_owned());
        let offers = runtime.block_on(client.search_offers(policy, 1))?;
        let offer = offers
            .into_iter()
            .next()
            .ok_or_else(|| "no Vast.ai offers available".to_owned())?;
        Ok(OfferPreview::from_offer(offer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swactor_vastai::Offer;

    #[test]
    fn vastai_offer_preview_preserves_offer_identity_price_and_vram_megabytes() {
        let preview = OfferPreview::from_offer(Offer {
            id: 42,
            gpu_name: "RTX 4090".to_owned(),
            dph_total: 0.375,
            gpu_ram: Some(23.5),
            geolocation: Some("US".to_owned()),
            inet_down_cost_per_tb: 0.0,
            inet_up_cost_per_tb: 0.0,
            host_id: Some(9001),
            verification: Some("verified".to_owned()),
        });

        assert_eq!(
            preview,
            OfferPreview {
                offer_id: 42,
                host_id: Some(9001),
                gpu_name: "RTX 4090".to_owned(),
                gpu_ram_mb: Some(24_064),
                dollars_per_hour: 0.375,
            }
        );
    }
}
