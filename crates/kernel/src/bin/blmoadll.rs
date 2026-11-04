use std::path::PathBuf;
use std::process::ExitCode;

use kernel::run;

fn usage() -> ExitCode {
    eprintln!("usage: blmoadll <config.toml> [--check] [--json]");
    ExitCode::from(2)
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut path: Option<PathBuf> = None;
    let mut check = false;
    let mut json = false;

    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--check" => check = true,
            "--json" => json = true,
            other if other.starts_with('-') => {
                eprintln!("blmoadll: unknown option `{other}`");
                return usage();
            }
            other => {
                if path.replace(PathBuf::from(other)).is_some() {
                    eprintln!("blmoadll: expected exactly one config path");
                    return usage();
                }
            }
        }
    }

    let Some(path) = path else {
        return usage();
    };

    let report = run(&path, check).await;

    if check {
        if json {
            match serde_json::to_string_pretty(&report.to_json()) {
                Ok(text) => println!("{text}"),
                Err(error) => {
                    eprintln!("blmoadll: cannot encode the report: {error}");
                    return ExitCode::from(1);
                }
            }
        } else {
            print!("{}", report.to_text());
        }
    }

    ExitCode::from(u8::try_from(report.code).unwrap_or(1))
}
