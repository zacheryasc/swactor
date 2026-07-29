//! MVP operator chat wrapper public surface.

pub mod config;
mod node_image;
mod runtime;

pub(super) fn run_from_args<I>(args: I) -> std::process::ExitCode
where
    I: IntoIterator<Item = String>,
{
    runtime::run_from_args(args)
}
