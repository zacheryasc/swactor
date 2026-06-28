use datastream::frame::{Frame, StreamId};
use serde_json::{Value, json};

use crate::FrameEvent;
use crate::view::DashboardView;

const LIVE_EXPLORER_HTML: &str = include_str!("live_explorer_page.html");

#[derive(Default)]
pub struct LiveDatastreamExplorer;

impl DashboardView for LiveDatastreamExplorer {
    fn id(&self) -> &'static str {
        "datastream/live"
    }

    fn title(&self) -> &'static str {
        "Datastream live explorer"
    }

    fn channels(&self) -> &'static [&'static str] {
        &[]
    }

    fn ingest(&self, _stream: &StreamId, _frame: &Frame, _event: &FrameEvent) {}

    fn snapshot_json(&self) -> Value {
        json!({
            "description": "Universal live explorer over /events and /api/frames",
            "page": "/view/datastream/live"
        })
    }

    fn html(&self) -> Option<&'static str> {
        Some(LIVE_EXPLORER_HTML)
    }
}
