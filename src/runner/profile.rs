use crate::config::{Config, Platform};
use std::collections::HashSet;

/// Resolve a profile into the full set of processes (services + tasks) to run,
/// including transitive dependencies. Starting with the profile's explicit
/// services and tasks, walks `depends_on` recursively to include everything
/// needed.
pub fn resolve_profile_processes(
    config: &Config,
    profile: &crate::config::Profile,
) -> HashSet<String> {
    resolve_profile_processes_inner(config, profile, None)
}

pub(in crate::runner) fn resolve_profile_processes_for_platform(
    config: &Config,
    profile: &crate::config::Profile,
    platform: Platform,
) -> HashSet<String> {
    resolve_profile_processes_inner(config, profile, Some(platform))
}

fn resolve_profile_processes_inner(
    config: &Config,
    profile: &crate::config::Profile,
    platform: Option<Platform>,
) -> HashSet<String> {
    let mut result = HashSet::new();
    let mut queue = config.expand_profile_services(&profile.services);
    queue.extend(profile.tasks.iter().cloned());

    while let Some(name) = queue.pop() {
        if !result.insert(name.clone()) {
            continue; // already visited
        }
        // Follow deps from services.
        if let Some(svc) = config.services.get(&name) {
            let service_deps = match platform {
                Some(platform) => {
                    config.effective_depends_on(&name, &svc.resolve(platform).depends_on)
                }
                None => config.effective_depends_on(&name, &svc.depends_on),
            };
            for dep in &service_deps {
                if !result.contains(&dep.name) {
                    queue.push(dep.name.clone());
                }
            }
        }
        // Follow deps from tasks.
        if let Some(task) = config.tasks.get(&name) {
            for dep in &config.effective_depends_on(&name, &task.depends_on) {
                if !result.contains(&dep.name) {
                    queue.push(dep.name.clone());
                }
            }
        }
    }
    result
}
