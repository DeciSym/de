// This file handles the check subcommand
use log::debug;
use std::{io, process::Command, process::Stdio};

pub struct Dependency {
    pub value: String,
    pub dep_type: DependencyType,
    pub required: bool,
}

pub enum DependencyType {
    File,
    Env,
    Binary,
}

// Function to do a basic health check of environment, confirming all dependencies are present
pub fn do_check(additional_deps: Option<Vec<Dependency>>) -> anyhow::Result<String, anyhow::Error> {
    let mut deps = vec![Dependency {
        value: "rdf2hdt".to_string(),
        dep_type: DependencyType::Binary,
        required: true,
    }];
    match additional_deps {
        None => {}
        Some(dependencies) => {
            deps.extend(dependencies);
        }
    };
    check_deps(deps)?;

    Ok("Ready to go".to_string())
}

pub fn check_deps(deps: Vec<Dependency>) -> anyhow::Result<(), anyhow::Error> {
    println!("Checking engine dependencies ...");
    let mut err_found = false;
    let mut warn_found = false;
    for dep in deps {
        match dep.dep_type {
            DependencyType::Binary => {
                match Command::new(dep.value.clone())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                {
                    core::result::Result::Ok(_) => println!("{}: found", dep.value),
                    core::result::Result::Err(err) => {
                        if let io::ErrorKind::NotFound = err.kind() {
                            log_warn_or_error(dep.required, format!("{}: MISSING", dep.value));
                        } else {
                            eprintln!("error executing dependency binary {}: {err}", dep.value);
                        }
                        if dep.required {
                            err_found = true;
                        } else {
                            warn_found = true
                        }
                    }
                }
            }
            DependencyType::Env => match std::env::var(dep.value.clone()) {
                Ok(s) => {
                    if !std::path::Path::new(&s).exists() {
                        if dep.required {
                            err_found = true;
                        } else {
                            warn_found = true
                        }
                        log_warn_or_error(
                            dep.required,
                            format!("{} variable points to non-existent file: {s}", dep.value),
                        );
                    } else {
                        println!("{}: found", dep.value);
                    }
                }
                Err(_) => {
                    if dep.required {
                        err_found = true;
                    } else {
                        warn_found = true
                    }
                    log_warn_or_error(
                        dep.required,
                        format!("{} environment variable missing", dep.value),
                    );
                }
            },
            DependencyType::File => {
                if !std::path::Path::new(&dep.value).exists() {
                    if dep.required {
                        err_found = true;
                    } else {
                        warn_found = true
                    }
                    log_warn_or_error(dep.required, format!("{}: MISSING", dep.value));
                } else {
                    println!("{}: found", dep.value);
                }
            }
        }
    }

    if warn_found {
        debug!("Some dependencies were not detected, this may result in loss of functionality for select CLI subcommands. Refer to README or man pages documentation for more info")
    }
    if err_found {
        return Err(anyhow::anyhow!("Please resolve missing dependencies"));
    }

    Ok(())
}

fn log_warn_or_error(required: bool, message: String) {
    if required {
        eprintln!("REQUIRED {message}");
    } else {
        debug!("OPTIONAL {message}");
    }
}
