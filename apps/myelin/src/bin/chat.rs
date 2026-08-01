fn main() -> std::process::ExitCode {
    myelin::run_chat_from_args(std::env::args().skip(1))
}
