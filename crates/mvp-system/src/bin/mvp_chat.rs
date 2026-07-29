fn main() -> std::process::ExitCode {
    mvp_system::run_chat_from_args(std::env::args().skip(1))
}
