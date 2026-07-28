fn main() -> std::process::ExitCode {
    mvp_system::chat::run_from_args(std::env::args().skip(1))
}
