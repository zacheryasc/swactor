use std::sync::Arc;

use crate::view::DashboardView;

mod actor_view;
mod worker_page;
mod worker_view;

pub use worker_view::SwactorWorkerView;
pub use actor_view::ActorPanelView;

pub const RUNTIME_STATS: &str = "runtime.stats";
pub const RUNTIME_WORKERS: &str = "runtime.workers";
pub const RUNTIME_ACTORS: &str = "runtime.actors";

pub fn worker_view() -> Arc<dyn DashboardView> {
    Arc::new(SwactorWorkerView::default())
}

/// Built-in actor overview + roster view (`/view/swactor/actor-overview`).
pub fn actor_overview_view() -> Arc<dyn DashboardView> {
    Arc::new(ActorPanelView::overview())
}

/// Built-in actor dossier view (`/view/swactor/actor-dossier`).
pub fn actor_dossier_view() -> Arc<dyn DashboardView> {
    Arc::new(ActorPanelView::dossier())
}
