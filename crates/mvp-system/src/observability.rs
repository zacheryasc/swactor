//! MVP observability public surface.

pub mod frame_archive;

pub mod benchmark_observability {
    pub use crate::benchmark_observability::*;
}

pub mod dashboard_view {
    pub use crate::dashboard_view::*;
}

pub mod observability_surface {
    pub use crate::observability_surface::*;
}

pub mod telemetry {
    pub use crate::telemetry::*;
}
