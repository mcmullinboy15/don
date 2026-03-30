use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "don", about = "Boss of your dev environment")]
struct Cli {
    /// Path to the config file
    #[arg(short, long, default_value = "don.toml")]
    config: PathBuf,
}

fn main() {
    let cli = Cli::parse();

    let config = don::config::Config::from_file(&cli.config).unwrap_or_else(|e| {
        eprintln!("Failed to load config from {}: {e}", cli.config.display());
        std::process::exit(1);
    });

    let platform = don::config::Platform::current().unwrap_or_else(|| {
        eprintln!(
            "Unsupported platform: {}-{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        );
        std::process::exit(1);
    });

    if let Err(errors) = config.validate(platform) {
        for e in &errors {
            eprintln!("config error: {e}");
        }
        std::process::exit(1);
    }

    println!("Loaded {} service(s)", config.services.len());
}
