use clap::Parser;

#[derive(clap::Parser)]
#[command(name = "kafcli", version)]
struct CommandLineArguments {}

fn main() {
    CommandLineArguments::parse();
}
