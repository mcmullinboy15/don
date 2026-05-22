//! Per-target dependency graph parsed from `bazel query ... --output=xml`.
//!
//! A single union query (`deps(T1 + T2 + ... + Tn)`) returns every rule and
//! source file reachable from any of the given targets. We stream-parse the
//! XML into an adjacency list keyed by rule label, then DFS per target to
//! get its own set of first-party source files (and thus watch packages).
//!
//! External (`@...`-prefixed) labels are skipped during parsing: we're only
//! watching first-party source files, and first-party rules never reach
//! first-party sources through an external intermediary in practice.

use std::collections::{HashMap, HashSet};
use std::io::BufRead;

use quick_xml::Reader;
use quick_xml::XmlVersion;
use quick_xml::events::{BytesStart, Event};

use crate::build_tool::BuildToolError;

const TOOL: &str = "bazel";

/// Parsed first-party slice of a Bazel dependency graph.
pub(super) struct BazelDepGraph {
    /// Rule label -> deduplicated list of direct `rule-input` labels.
    rules: HashMap<String, Vec<String>>,
    /// First-party source file labels seen in the XML.
    sources: HashSet<String>,
}

impl BazelDepGraph {
    /// Parse a streaming `bazel query ... --output=xml` document.
    ///
    /// Entries whose labels start with `@` are skipped; their `rule-input`
    /// edges are dropped on the floor. DFS walks that cross an external
    /// rule terminate there, which is what we want.
    pub(super) fn parse_xml<R: BufRead>(reader: R) -> Result<Self, BuildToolError> {
        let mut xml = Reader::from_reader(reader);
        xml.config_mut().trim_text(true);

        let mut rules: HashMap<String, Vec<String>> = HashMap::new();
        let mut sources: HashSet<String> = HashSet::new();
        let mut buf = Vec::new();

        // `Some` when we're inside a first-party `<rule>` element and should
        // collect its `rule-input` children. `None` otherwise (including inside
        // external rules we skipped).
        let mut current: Option<(String, Vec<String>, HashSet<String>)> = None;

        loop {
            match xml.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => match e.name().as_ref() {
                    b"rule" => {
                        let name = read_name(e)?;
                        if let Some(name) = name
                            && !is_external(&name)
                        {
                            current = Some((name, Vec::new(), HashSet::new()));
                        }
                    }
                    b"source-file" => {
                        if let Some(name) = read_name(e)?
                            && !is_external(&name)
                        {
                            sources.insert(name);
                        }
                    }
                    _ => {}
                },
                Ok(Event::Empty(ref e)) => {
                    match e.name().as_ref() {
                        b"rule-input" => {
                            if let Some((_, inputs, seen)) = current.as_mut()
                                && let Some(name) = read_name(e)?
                                && seen.insert(name.clone())
                            {
                                inputs.push(name);
                            }
                        }
                        b"source-file" => {
                            // Rare but legal: an empty `<source-file/>` element
                            // (no visibility-label child).
                            if let Some(name) = read_name(e)?
                                && !is_external(&name)
                            {
                                sources.insert(name);
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Event::End(ref e)) => {
                    if e.name().as_ref() == b"rule"
                        && let Some((name, inputs, _)) = current.take()
                    {
                        rules.insert(name, inputs);
                    }
                }
                Ok(Event::Eof) => break,
                Err(err) => {
                    return Err(BuildToolError::ParseError {
                        tool: TOOL.to_string(),
                        message: format!("xml parse error: {err}"),
                    });
                }
                _ => {}
            }
            buf.clear();
        }

        Ok(Self { rules, sources })
    }

    /// First-party package directories that feed into `target`.
    ///
    /// DFS from `target` through `rule-input` edges, collecting every
    /// first-party source file reached. Each source file contributes its
    /// package (the path portion of its label).
    ///
    /// Returns sorted, deduplicated package paths. Empty if the target is
    /// unknown or has no first-party sources.
    ///
    /// Two package paths are filtered out even if reachable:
    /// - **The empty (root) package.** A service that reaches `//:foo.txt`
    ///   would otherwise contribute `""`, which downstream becomes `/**` —
    ///   an absolute pattern matching the entire filesystem. Root-level
    ///   BUILD / WORKSPACE / MODULE.bazel files are already covered by the
    ///   tier-1 build-graph watch; other root-level source files lose
    ///   coverage here, and that's the right tradeoff given the blast
    ///   radius of watching the whole repo root.
    /// - **`bazel-out/...` packages.** These are generated-file outputs,
    ///   not source files we should watch.
    pub(super) fn packages_for(&self, target: &str) -> Vec<String> {
        let mut visited: HashSet<&str> = HashSet::new();
        let mut stack: Vec<&str> = vec![target];
        let mut packages: HashSet<&str> = HashSet::new();

        while let Some(label) = stack.pop() {
            if !visited.insert(label) {
                continue;
            }
            if self.sources.contains(label) {
                if let Some(pkg) = package_of(label)
                    && !pkg.is_empty()
                    && !pkg.starts_with("bazel-out")
                {
                    packages.insert(pkg);
                }
                continue;
            }
            if let Some(deps) = self.rules.get(label) {
                for d in deps {
                    stack.push(d.as_str());
                }
            }
            // Missing rule (e.g. external, pruned during parse) terminates
            // the walk for this path. Intentional.
        }

        let mut out: Vec<String> = packages.into_iter().map(String::from).collect();
        out.sort();
        out
    }
}

fn is_external(label: &str) -> bool {
    label.starts_with('@')
}

/// Extract the package portion of a Bazel label:
/// `//a/b/c:target` -> `a/b/c`; `//a/b/c` -> `a/b/c`. Returns `None` if the
/// label is not absolute (`@` or relative).
fn package_of(label: &str) -> Option<&str> {
    let stripped = label.strip_prefix("//")?;
    let end = stripped.find(':').unwrap_or(stripped.len());
    Some(&stripped[..end])
}

/// Read the `name` attribute from an XML element, XML-unescaped.
///
/// Bazel labels never contain `&`/`<`/`>` in practice, but the XML format is
/// specified so we unescape defensively.
fn read_name(elem: &BytesStart<'_>) -> Result<Option<String>, BuildToolError> {
    for attr in elem.attributes() {
        let attr = attr.map_err(|e| BuildToolError::ParseError {
            tool: TOOL.to_string(),
            message: format!("xml attribute parse error: {e}"),
        })?;
        if attr.key.as_ref() == b"name" {
            let unescaped = attr
                .normalized_value(XmlVersion::Implicit1_0)
                .map_err(|e| BuildToolError::ParseError {
                    tool: TOOL.to_string(),
                    message: format!("xml attribute decode error: {e}"),
                })?;
            return Ok(Some(unescaped.into_owned()));
        }
    }
    Ok(None)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn parse(xml: &str) -> BazelDepGraph {
        BazelDepGraph::parse_xml(xml.as_bytes()).unwrap()
    }

    #[test]
    fn test_package_of() {
        struct Case {
            name: &'static str,
            input: &'static str,
            expected: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "label with target",
                input: "//a/b/c:target",
                expected: Some("a/b/c"),
            },
            Case {
                name: "label without target",
                input: "//a/b/c",
                expected: Some("a/b/c"),
            },
            Case {
                name: "root package with target",
                input: "//:root",
                expected: Some(""),
            },
            Case {
                name: "nested source path",
                input: "//a/b:sub/dir/file.sh",
                expected: Some("a/b"),
            },
            Case {
                name: "external label",
                input: "@repo//a:b",
                expected: None,
            },
            Case {
                name: "relative label",
                input: ":target",
                expected: None,
            },
        ];

        for c in cases {
            assert_eq!(package_of(c.input), c.expected, "case: {}", c.name);
        }
    }

    #[test]
    fn test_parse_basic_graph() {
        let xml = r#"<?xml version="1.1"?>
<query version="2">
    <source-file name="//pkg/a:src.go">
        <visibility-label name="//visibility:public"/>
    </source-file>
    <rule class="go_binary" name="//pkg/a:bin">
        <string name="name" value="bin"/>
        <rule-input name="//pkg/a:src.go"/>
        <rule-input name="//pkg/b:lib"/>
    </rule>
    <source-file name="//pkg/b:lib.go"/>
    <rule class="go_library" name="//pkg/b:lib">
        <rule-input name="//pkg/b:lib.go"/>
    </rule>
</query>"#;

        let g = parse(xml);
        assert_eq!(g.rules.len(), 2);
        assert_eq!(g.sources.len(), 2);
        assert!(g.sources.contains("//pkg/a:src.go"));
        assert!(g.sources.contains("//pkg/b:lib.go"));

        let packages = g.packages_for("//pkg/a:bin");
        assert_eq!(packages, vec!["pkg/a".to_string(), "pkg/b".to_string()]);

        let packages = g.packages_for("//pkg/b:lib");
        assert_eq!(packages, vec!["pkg/b".to_string()]);
    }

    #[test]
    fn test_filters_external_labels() {
        let xml = r#"<?xml version="1.1"?>
<query version="2">
    <source-file name="//pkg/a:src.go"/>
    <source-file name="@rules_go//foo:ext.go"/>
    <rule class="go_binary" name="//pkg/a:bin">
        <rule-input name="//pkg/a:src.go"/>
        <rule-input name="@rules_go//foo:dep"/>
    </rule>
    <rule class="go_library" name="@rules_go//foo:dep">
        <rule-input name="@rules_go//foo:ext.go"/>
    </rule>
</query>"#;

        let g = parse(xml);
        assert_eq!(g.rules.len(), 1, "only first-party rules kept");
        assert_eq!(g.sources.len(), 1, "only first-party sources kept");
        assert!(g.sources.contains("//pkg/a:src.go"));

        // Even though //pkg/a:bin has an edge to @rules_go//foo:dep, that edge
        // terminates at a missing rule — DFS stops and we don't accidentally
        // pick up external sources.
        let packages = g.packages_for("//pkg/a:bin");
        assert_eq!(packages, vec!["pkg/a".to_string()]);
    }

    #[test]
    fn test_dedup_rule_inputs() {
        let xml = r#"<?xml version="1.1"?>
<query version="2">
    <source-file name="//pkg/a:src.go"/>
    <rule class="go_binary" name="//pkg/a:bin">
        <rule-input name="//pkg/a:src.go"/>
        <rule-input name="//pkg/a:src.go"/>
        <rule-input name="//pkg/a:src.go"/>
    </rule>
</query>"#;

        let g = parse(xml);
        let inputs = g.rules.get("//pkg/a:bin").unwrap();
        assert_eq!(inputs.len(), 1);
    }

    #[test]
    fn test_cycle_safe() {
        // Not a real Bazel scenario, but defensively handle it anyway.
        let xml = r#"<?xml version="1.1"?>
<query version="2">
    <source-file name="//pkg/a:src.go"/>
    <rule class="fake" name="//pkg/a:x">
        <rule-input name="//pkg/a:y"/>
        <rule-input name="//pkg/a:src.go"/>
    </rule>
    <rule class="fake" name="//pkg/a:y">
        <rule-input name="//pkg/a:x"/>
    </rule>
</query>"#;

        let g = parse(xml);
        // DFS with visited set must not loop.
        let packages = g.packages_for("//pkg/a:x");
        assert_eq!(packages, vec!["pkg/a".to_string()]);
    }

    #[test]
    fn test_filters_root_and_bazel_out_packages() {
        // A service that reaches //:config.yaml (root package) and
        // //bazel-out/k8-fastbuild/bin/gen:out.go (generated) plus a normal
        // first-party source. Only the normal one should survive.
        let xml = r#"<?xml version="1.1"?>
<query version="2">
    <source-file name="//:config.yaml"/>
    <source-file name="//bazel-out/k8-fastbuild/bin/gen:out.go"/>
    <source-file name="//pkg/a:src.go"/>
    <rule class="fake" name="//pkg/a:bin">
        <rule-input name="//:config.yaml"/>
        <rule-input name="//bazel-out/k8-fastbuild/bin/gen:out.go"/>
        <rule-input name="//pkg/a:src.go"/>
    </rule>
</query>"#;

        let g = parse(xml);
        let packages = g.packages_for("//pkg/a:bin");
        assert_eq!(
            packages,
            vec!["pkg/a".to_string()],
            "root `` and bazel-out packages must be filtered out — otherwise watch paths become /** and watch the whole filesystem"
        );
    }

    #[test]
    fn test_unknown_target() {
        let xml = r#"<?xml version="1.1"?>
<query version="2">
    <source-file name="//pkg/a:src.go"/>
    <rule class="go_binary" name="//pkg/a:bin">
        <rule-input name="//pkg/a:src.go"/>
    </rule>
</query>"#;

        let g = parse(xml);
        assert!(g.packages_for("//nope:missing").is_empty());
    }

    #[test]
    fn test_ignores_attribute_name_collisions() {
        // `<string name="name">` is a rule *attribute* named "name", not the
        // rule's identity. We must ignore these since we only match outer
        // `<rule>` / `<rule-input>` / `<source-file>` tags.
        let xml = r#"<?xml version="1.1"?>
<query version="2">
    <source-file name="//pkg/a:src.go"/>
    <rule class="go_binary" name="//pkg/a:bin">
        <string name="name" value="bin"/>
        <label name="main" value="//pkg/a:src.go"/>
        <list name="data">
            <label value="//pkg/a:other"/>
        </list>
        <rule-input name="//pkg/a:src.go"/>
    </rule>
</query>"#;

        let g = parse(xml);
        assert_eq!(g.rules.len(), 1);
        let inputs = g.rules.get("//pkg/a:bin").unwrap();
        assert_eq!(inputs, &vec!["//pkg/a:src.go".to_string()]);
    }

    // Smoke test against a real Bazel XML dump.
    //
    // Set `DON_BAZEL_XML_FIXTURE=/path/to/deps.xml` to enable. Off by default
    // because the test expects a specific shape.
    #[test]
    fn smoke_real_xml() {
        let Ok(path) = std::env::var("DON_BAZEL_XML_FIXTURE") else {
            return;
        };
        let bytes = std::fs::read(&path).unwrap();
        let start = std::time::Instant::now();
        let g = BazelDepGraph::parse_xml(bytes.as_slice()).unwrap();
        let parse_time = start.elapsed();
        eprintln!(
            "parsed {} bytes in {:?}: {} rules, {} sources",
            bytes.len(),
            parse_time,
            g.rules.len(),
            g.sources.len(),
        );
        let valkey = g.packages_for("//redo/valkey/server:listen");
        let api = g.packages_for("//redo/api/server:listen");
        eprintln!("valkey packages: {} -> {:?}", valkey.len(), valkey);
        eprintln!("api packages:    {}", api.len());
        for p in &valkey.iter().chain(api.iter()).collect::<Vec<_>>() {
            assert!(!p.is_empty(), "empty package leaked through");
            assert!(
                !p.starts_with("bazel-out"),
                "bazel-out package leaked through"
            );
        }
        // valkey should have far fewer packages than api.
        assert!(
            valkey.len() < api.len(),
            "valkey ({}) should have fewer packages than api ({})",
            valkey.len(),
            api.len(),
        );
        assert!(
            !valkey.is_empty(),
            "valkey should have at least one package"
        );
    }

    #[test]
    fn test_diamond_dependency() {
        // a -> b, a -> c, b -> d, c -> d — d's package should appear once.
        let xml = r#"<?xml version="1.1"?>
<query version="2">
    <source-file name="//pkg/d:src.go"/>
    <rule class="fake" name="//pkg/a:x">
        <rule-input name="//pkg/b:x"/>
        <rule-input name="//pkg/c:x"/>
    </rule>
    <rule class="fake" name="//pkg/b:x">
        <rule-input name="//pkg/d:x"/>
    </rule>
    <rule class="fake" name="//pkg/c:x">
        <rule-input name="//pkg/d:x"/>
    </rule>
    <rule class="fake" name="//pkg/d:x">
        <rule-input name="//pkg/d:src.go"/>
    </rule>
</query>"#;

        let g = parse(xml);
        let packages = g.packages_for("//pkg/a:x");
        assert_eq!(packages, vec!["pkg/d".to_string()]);
    }
}
