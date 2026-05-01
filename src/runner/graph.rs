use super::Runner;
use std::collections::{HashMap, HashSet, VecDeque};

/// Topologically sort a dependency graph.
///
/// Returns node names in an order where every node appears after all
/// its dependencies. Nodes at the same depth can be started in parallel.
///
/// Uses Kahn's algorithm (BFS-based). Returns `Err` with the cycle path
/// if a cycle is detected.
pub(crate) fn topological_sort(
    deps: &HashMap<String, Vec<String>>,
) -> Result<Vec<String>, Vec<String>> {
    // Build in-degree map and reverse adjacency list.
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

    for name in deps.keys() {
        in_degree.entry(name.as_str()).or_insert(0);
    }

    for (name, node_deps) in deps {
        for dep in node_deps {
            in_degree.entry(dep.as_str()).or_insert(0);
            dependents
                .entry(dep.as_str())
                .or_default()
                .push(name.as_str());
            *in_degree.entry(name.as_str()).or_insert(0) += 1;
        }
    }

    // Seed queue with nodes that have no dependencies.
    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(&name, _)| name)
        .collect();

    // Sort the queue for deterministic output.
    let mut sorted_queue: Vec<&str> = queue.drain(..).collect();
    sorted_queue.sort();
    queue.extend(sorted_queue);

    let mut result = Vec::new();

    while let Some(node) = queue.pop_front() {
        result.push(node.to_string());
        if let Some(children) = dependents.get(node) {
            let mut ready_children = Vec::new();
            for &child in children {
                if let Some(deg) = in_degree.get_mut(child) {
                    *deg -= 1;
                    if *deg == 0 {
                        ready_children.push(child);
                    }
                }
            }
            // Sort for determinism.
            ready_children.sort();
            queue.extend(ready_children);
        }
    }

    if result.len() != deps.len() {
        // Cycle detected — find the cycle path for error reporting.
        let remaining: Vec<String> = deps
            .keys()
            .filter(|k| !result.contains(k))
            .cloned()
            .collect();
        // Walk the remaining nodes to find the cycle.
        if let Some(cycle) = find_cycle(deps, &remaining) {
            return Err(cycle);
        }
        // Fallback: return the remaining nodes as the "cycle".
        return Err(remaining);
    }

    Ok(result)
}

/// Find a cycle in the dependency graph among the given candidate nodes.
fn find_cycle(deps: &HashMap<String, Vec<String>>, candidates: &[String]) -> Option<Vec<String>> {
    let candidate_set: HashSet<&str> = candidates.iter().map(|s| s.as_str()).collect();

    #[derive(Clone, Copy, PartialEq)]
    enum State {
        Unvisited,
        Visiting,
        Visited,
    }

    let mut state: HashMap<&str, State> = candidates
        .iter()
        .map(|n| (n.as_str(), State::Unvisited))
        .collect();
    let mut path: Vec<String> = Vec::new();

    fn dfs<'a>(
        node: &'a str,
        deps: &'a HashMap<String, Vec<String>>,
        state: &mut HashMap<&'a str, State>,
        path: &mut Vec<String>,
        candidates: &HashSet<&str>,
    ) -> Option<Vec<String>> {
        if let Some(s) = state.get_mut(node) {
            *s = State::Visiting;
        }
        path.push(node.to_string());

        if let Some(node_deps) = deps.get(node) {
            for dep in node_deps {
                if !candidates.contains(dep.as_str()) {
                    continue;
                }
                match state.get(dep.as_str()) {
                    Some(State::Visiting) => {
                        if let Some(cycle_start) = path.iter().position(|n| n == dep) {
                            let mut cycle: Vec<String> = path[cycle_start..].to_vec();
                            cycle.push(dep.clone());
                            return Some(cycle);
                        }
                    }
                    Some(State::Unvisited) | None => {
                        if let Some(cycle) = dfs(dep, deps, state, path, candidates) {
                            return Some(cycle);
                        }
                    }
                    Some(State::Visited) => {}
                }
            }
        }

        path.pop();
        if let Some(s) = state.get_mut(node) {
            *s = State::Visited;
        }
        None
    }

    for candidate in candidates {
        if state.get(candidate.as_str()) == Some(&State::Unvisited)
            && let Some(cycle) = dfs(candidate, deps, &mut state, &mut path, &candidate_set)
        {
            return Some(cycle);
        }
    }

    None
}

/// Compute the topological depth of each node (for parallel execution ordering).
/// Depth 0 = no dependencies. Higher depth = must wait for deeper nodes.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
pub(crate) fn compute_depths(
    order: &[String],
    deps: &HashMap<String, Vec<String>>,
) -> HashMap<String, usize> {
    let mut depths: HashMap<String, usize> = HashMap::new();
    for name in order {
        let node_deps = deps.get(name).cloned().unwrap_or_default();
        let max_dep_depth = node_deps
            .iter()
            .filter_map(|d| depths.get(d.as_str()))
            .max()
            .copied()
            .unwrap_or(0);
        let depth = if node_deps.is_empty() {
            0
        } else {
            max_dep_depth + 1
        };
        depths.insert(name.clone(), depth);
    }
    depths
}

impl Runner {
    pub(in crate::runner) fn build_dep_map(&self) -> HashMap<String, Vec<String>> {
        let mut deps = HashMap::new();
        for (name, rs) in &self.services {
            deps.insert(name.clone(), rs.resolved.depends_on.clone());
        }
        for (name, rt) in &self.tasks {
            deps.insert(name.clone(), rt.config.depends_on.clone());
        }
        deps
    }
}
