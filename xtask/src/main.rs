use std::process::Command;
use std::time::Instant;

struct TestStep {
    label: &'static str,
    args: &'static [&'static str],
}

struct Group {
    name: &'static str,
    description: &'static str,
    steps: &'static [TestStep],
}

const CORE: Group = Group {
    name: "core",
    description: "Actor runtime, message delivery, property tests",
    steps: &[TestStep {
        label: "actor runtime",
        args: &["test", "-p", "swactor", "--features", "transport"],
    }],
};

const DISTRIBUTION: Group = Group {
    name: "distribution",
    description: "Distribution protocol + datastore",
    steps: &[
        TestStep {
            label: "distribution protocol",
            args: &["test", "-p", "distribution"],
        },
        TestStep {
            label: "datastore",
            args: &["test", "-p", "swactor-datastore"],
        },
    ],
};

const CLUSTER_SIMS: Group = Group {
    name: "cluster-sims",
    description: "Deterministic cluster simulations",
    steps: &[TestStep {
        label: "cluster simulations",
        args: &["test", "-p", "simulation"],
    }],
};

const INTEGRATED: Group = Group {
    name: "integrated",
    description: "HTTP API + dashboard end-to-end tests",
    steps: &[
        TestStep {
            label: "datastore integration (node features)",
            args: &[
                "test",
                "-p",
                "swactor-datastore",
                "--features",
                "node",
                "--test",
                "api_integration_test",
                "--test",
                "dashboard_integration_test",
            ],
        },
        TestStep {
            label: "runtime dashboard",
            args: &["test", "-p", "runtime-dashboard"],
        },
    ],
};

fn groups_for(name: &str) -> Option<Vec<&'static Group>> {
    match name {
        "core" => Some(vec![&CORE]),
        "distribution" => Some(vec![&DISTRIBUTION]),
        "cluster-sims" => Some(vec![&CLUSTER_SIMS]),
        "integrated" => Some(vec![&INTEGRATED]),
        "essential" => Some(vec![&CORE, &DISTRIBUTION, &INTEGRATED]),
        "all" => Some(vec![&CORE, &DISTRIBUTION, &CLUSTER_SIMS, &INTEGRATED]),
        _ => None,
    }
}

fn run_step(group_name: &str, step: &TestStep) -> bool {
    println!("\n=== {group_name}: {} ===", step.label);
    println!("    cargo {}", step.args.join(" "));
    println!();

    let status = Command::new("cargo")
        .args(step.args)
        .status();

    match status {
        Ok(s) => s.success(),
        Err(e) => {
            eprintln!("Failed to execute cargo: {e}");
            false
        }
    }
}

fn print_usage() {
    println!(
        "\
USAGE: cargo xtask test <GROUP>

GROUPS:
  core          Actor runtime, message delivery, property tests
  distribution  Distribution protocol + datastore
  cluster-sims  Deterministic cluster simulations
  integrated    HTTP API + dashboard end-to-end tests
  essential     core + distribution + integrated (merge gate)
  all           Every test group

FLAGS:
  --list        Show all groups and the cargo commands they run"
    );
}

fn print_list() {
    let all_groups: &[(&[&str], &Group)] = &[
        (&[], &CORE),
        (&[], &DISTRIBUTION),
        (&[], &CLUSTER_SIMS),
        (&[], &INTEGRATED),
    ];

    println!("Available test groups:\n");

    for &(_, group) in all_groups {
        println!("  {:<14}{}", group.name, group.description);
        for step in group.steps {
            println!("                → cargo {}", step.args.join(" "));
        }
        println!();
    }

    println!("  {:<14}core + distribution + integrated (merge gate)", "essential");
    println!("  {:<14}Every test group", "all");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 || args[1] != "test" {
        print_usage();
        std::process::exit(if args.len() < 2 { 1 } else { 1 });
    }

    if args.len() < 3 {
        print_usage();
        std::process::exit(1);
    }

    let target = &args[2];

    if target == "--list" {
        print_list();
        return;
    }

    let groups = match groups_for(target) {
        Some(g) => g,
        None => {
            eprintln!("Unknown test group: {target}\n");
            print_usage();
            std::process::exit(1);
        }
    };

    let start = Instant::now();
    let mut passed = 0usize;
    let mut failed = 0usize;

    for group in &groups {
        for step in group.steps {
            if run_step(group.name, step) {
                passed += 1;
            } else {
                failed += 1;
                let elapsed = start.elapsed();
                println!(
                    "\n--- FAILED after {:.1}s ({passed} passed, {failed} failed) ---",
                    elapsed.as_secs_f64()
                );
                std::process::exit(1);
            }
        }
    }

    let elapsed = start.elapsed();
    println!(
        "\n--- All {passed} step(s) passed in {:.1}s ---",
        elapsed.as_secs_f64()
    );
}
