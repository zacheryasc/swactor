use std::sync::Arc;

use crate::view::DashboardView;

mod worker_page;
mod worker_view;

pub use worker_view::SwactorWorkerView;

pub const RUNTIME_STATS: &str = "runtime.stats";
pub const RUNTIME_WORKERS: &str = "runtime.workers";
pub const RUNTIME_ACTORS: &str = "runtime.actors";

pub fn worker_view() -> Arc<dyn DashboardView> {
    Arc::new(SwactorWorkerView::default())
}
