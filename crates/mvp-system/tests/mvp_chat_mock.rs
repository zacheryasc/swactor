fn main() -> std::process::ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        return std::process::ExitCode::SUCCESS;
    }
    mvp_system::chat::run_from_args(args)
}
