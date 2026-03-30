use std::fmt::Write;
use std::path::{Path, PathBuf};

/// Builder for programmatically generating `don.toml` content for tests.
///
/// Services and tasks are built with sub-builders that allow chaining
/// `depends_on` and other fields before finalizing with `.done()`.
pub struct ConfigBuilder {
    toml: String,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self {
            toml: String::new(),
        }
    }

    /// Add a custom service with a run command. Call `.done()` to finalize.
    pub fn add_custom_service(self, name: &str, cmd: &str, args: &[&str]) -> ServiceBuilder {
        let mut lines = vec![format!("run.cmd = \"{cmd}\"")];
        if !args.is_empty() {
            let args_str: Vec<String> = args.iter().map(|a| format!("\"{a}\"")).collect();
            lines.push(format!("run.args = [{}]", args_str.join(", ")));
        }
        ServiceBuilder {
            builder: self,
            name: name.to_string(),
            lines,
        }
    }

    /// Add a docker service. Call `.done()` to finalize.
    pub fn add_docker_service(self, name: &str, image: &str) -> ServiceBuilder {
        ServiceBuilder {
            builder: self,
            name: name.to_string(),
            lines: vec![format!("docker.image = \"{image}\"")],
        }
    }

    /// Add a rust service. Call `.done()` to finalize.
    pub fn add_rust_service(self, name: &str, binary: &str) -> ServiceBuilder {
        ServiceBuilder {
            builder: self,
            name: name.to_string(),
            lines: vec![format!("rust.binary = \"{binary}\"")],
        }
    }

    /// Add a task. Call `.done()` to finalize.
    pub fn add_task(self, name: &str, cmd: &str, args: &[&str]) -> TaskBuilder {
        let mut lines = vec![format!("cmd = \"{cmd}\"")];
        if !args.is_empty() {
            let args_str: Vec<String> = args.iter().map(|a| format!("\"{a}\"")).collect();
            lines.push(format!("args = [{}]", args_str.join(", ")));
        }
        TaskBuilder {
            builder: self,
            name: name.to_string(),
            lines,
        }
    }

    /// Add a profile.
    pub fn add_profile(mut self, name: &str, services: &[&str], tasks: &[&str]) -> Self {
        writeln!(self.toml, "[profiles.{name}]").unwrap();
        if !services.is_empty() {
            let svc_str: Vec<String> = services.iter().map(|s| format!("\"{s}\"")).collect();
            writeln!(self.toml, "services = [{}]", svc_str.join(", ")).unwrap();
        }
        if !tasks.is_empty() {
            let task_str: Vec<String> = tasks.iter().map(|t| format!("\"{t}\"")).collect();
            writeln!(self.toml, "tasks = [{}]", task_str.join(", ")).unwrap();
        }
        writeln!(self.toml).unwrap();
        self
    }

    /// Append raw TOML content.
    pub fn raw(mut self, toml: &str) -> Self {
        writeln!(self.toml, "{toml}").unwrap();
        self
    }

    /// Get the generated TOML as a string.
    pub fn build(&self) -> String {
        self.toml.clone()
    }

    /// Write the generated TOML to `don.toml` in the given directory.
    /// Returns the path to the written file.
    pub fn write_to(&self, dir: &Path) -> PathBuf {
        let path = dir.join("don.toml");
        std::fs::write(&path, &self.toml).unwrap();
        path
    }
}

/// Builder for a service entry. Call `.done()` to finalize and return to `ConfigBuilder`.
pub struct ServiceBuilder {
    builder: ConfigBuilder,
    name: String,
    lines: Vec<String>,
}

impl ServiceBuilder {
    /// Add depends_on to this service.
    pub fn depends_on(mut self, deps: &[&str]) -> Self {
        let deps_str: Vec<String> = deps.iter().map(|d| format!("\"{d}\"")).collect();
        self.lines
            .push(format!("depends_on = [{}]", deps_str.join(", ")));
        self
    }

    /// Finalize this service and return to the config builder.
    pub fn done(mut self) -> ConfigBuilder {
        writeln!(self.builder.toml, "[services.{}]", self.name).unwrap();
        for line in &self.lines {
            writeln!(self.builder.toml, "{line}").unwrap();
        }
        writeln!(self.builder.toml).unwrap();
        self.builder
    }

    // Convenience: allow writing directly without going back to ConfigBuilder
    /// Write the generated TOML to `don.toml` in the given directory.
    pub fn write_to(self, dir: &Path) -> PathBuf {
        self.done().write_to(dir)
    }
}

/// Builder for a task entry. Call `.done()` to finalize and return to `ConfigBuilder`.
pub struct TaskBuilder {
    builder: ConfigBuilder,
    name: String,
    lines: Vec<String>,
}

impl TaskBuilder {
    /// Add depends_on to this task.
    pub fn depends_on(mut self, deps: &[&str]) -> Self {
        let deps_str: Vec<String> = deps.iter().map(|d| format!("\"{d}\"")).collect();
        self.lines
            .push(format!("depends_on = [{}]", deps_str.join(", ")));
        self
    }

    /// Finalize this task and return to the config builder.
    pub fn done(mut self) -> ConfigBuilder {
        writeln!(self.builder.toml, "[tasks.{}]", self.name).unwrap();
        for line in &self.lines {
            writeln!(self.builder.toml, "{line}").unwrap();
        }
        writeln!(self.builder.toml).unwrap();
        self.builder
    }

    /// Write the generated TOML to `don.toml` in the given directory.
    pub fn write_to(self, dir: &Path) -> PathBuf {
        self.done().write_to(dir)
    }
}
