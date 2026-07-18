fn main() {
    mvp_system::mvp_chat::run_from_args(std::env::args().skip(1)).expect("failed");
}
