//! Dependency edges in `depends_on`.
//!
//! Every edge carries a *kind*. A **required** dependency (the default) gates
//! the dependent on the dependency reaching a satisfied state, and a failure
//! propagates: the dependent is skipped with `dep failed`. An **optional**
//! dependency is ordering only — the dependent still waits for it to settle,
//! but starts whether it succeeded or failed.
//!
//! Both forms are accepted in TOML:
//!
//! ```toml
//! depends_on = [
//!   "postgres",                                # required (default)
//!   { name = "otel-collector", required = false },  # ordering only
//! ]
//! ```

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// One `depends_on` edge: the name of a service, task, or service group,
/// plus whether the dependent may start when that dependency fails.
///
/// Deserializes from either a bare string (`"postgres"`, required) or a table
/// (`{ name = "otel", required = false }`). Serializes back to the same
/// shorthand: a required dependency is written as a plain string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Dependency {
    /// The referenced service, task, or service group.
    pub name: String,
    /// When `true` (the default), the dependency must become ready/complete
    /// before the dependent starts, and its failure skips the dependent.
    /// When `false`, the edge only orders startup: the dependent waits for
    /// the dependency to settle and then starts regardless of the outcome.
    pub required: bool,
}

impl Dependency {
    /// A required dependency — the default kind.
    pub fn required(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            required: true,
        }
    }

    /// An ordering-only dependency whose failure does not block the dependent.
    pub fn optional(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            required: false,
        }
    }

    /// The same edge kind pointed at a different name. Used when a service
    /// group reference expands to its members: each member inherits the
    /// requiredness declared on the group reference.
    pub(crate) fn with_name(&self, name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            required: self.required,
        }
    }
}

impl fmt::Display for Dependency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.required {
            write!(f, "{}", self.name)
        } else {
            write!(f, "{} (optional)", self.name)
        }
    }
}

impl From<&str> for Dependency {
    fn from(name: &str) -> Self {
        Self::required(name)
    }
}

impl From<String> for Dependency {
    fn from(name: String) -> Self {
        Self::required(name)
    }
}

impl Serialize for Dependency {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.required {
            return serializer.serialize_str(&self.name);
        }
        use serde::ser::SerializeStruct;
        let mut entry = serializer.serialize_struct("Dependency", 2)?;
        entry.serialize_field("name", &self.name)?;
        entry.serialize_field("required", &self.required)?;
        entry.end()
    }
}

impl<'de> Deserialize<'de> for Dependency {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(DependencyVisitor)
    }
}

struct DependencyVisitor;

impl<'de> Visitor<'de> for DependencyVisitor {
    type Value = Dependency;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a dependency name, or a table like { name = \"api\", required = false }")
    }

    fn visit_str<E>(self, value: &str) -> Result<Dependency, E>
    where
        E: serde::de::Error,
    {
        Ok(Dependency::required(value))
    }

    fn visit_map<M>(self, mut map: M) -> Result<Dependency, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut name: Option<String> = None;
        let mut required: Option<bool> = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "name" => name = Some(map.next_value()?),
                "required" => required = Some(map.next_value()?),
                other => {
                    return Err(serde::de::Error::custom(format!(
                        "unknown key '{other}' in depends_on entry — expected 'name' or 'required'"
                    )));
                }
            }
        }

        let name = name.ok_or_else(|| {
            serde::de::Error::custom("depends_on entry is missing 'name' (e.g. { name = \"api\" })")
        })?;

        Ok(Dependency {
            name,
            required: required.unwrap_or(true),
        })
    }
}

/// Append `dependency` to `list`, keeping the first position but the strictest
/// kind: if the same name is reachable both as a required and an optional
/// edge, the required edge wins.
pub(crate) fn push_dependency(list: &mut Vec<Dependency>, dependency: Dependency) {
    if let Some(existing) = list.iter_mut().find(|d| d.name == dependency.name) {
        existing.required |= dependency.required;
        return;
    }
    list.push(dependency);
}

/// Names only, in declaration order. Handy for topological sorting and for
/// call sites that don't care about the edge kind.
pub(crate) fn dependency_names(dependencies: &[Dependency]) -> Vec<String> {
    dependencies.iter().map(|d| d.name.clone()).collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct Holder {
        #[serde(default)]
        depends_on: Vec<Dependency>,
    }

    #[test]
    fn dependency_parse_cases() {
        struct Case {
            name: &'static str,
            toml: &'static str,
            want: Result<Vec<Dependency>, &'static str>,
        }

        let cases = vec![
            Case {
                name: "bare strings are required",
                toml: r#"depends_on = ["postgres", "migrate"]"#,
                want: Ok(vec![
                    Dependency::required("postgres"),
                    Dependency::required("migrate"),
                ]),
            },
            Case {
                name: "table with required = false is optional",
                toml: r#"depends_on = [{ name = "otel", required = false }]"#,
                want: Ok(vec![Dependency::optional("otel")]),
            },
            Case {
                name: "table without required defaults to required",
                toml: r#"depends_on = [{ name = "otel" }]"#,
                want: Ok(vec![Dependency::required("otel")]),
            },
            Case {
                name: "mixed forms keep declaration order",
                toml: r#"depends_on = ["db", { name = "otel", required = false }, "cache"]"#,
                want: Ok(vec![
                    Dependency::required("db"),
                    Dependency::optional("otel"),
                    Dependency::required("cache"),
                ]),
            },
            Case {
                name: "missing name is rejected",
                toml: r#"depends_on = [{ required = false }]"#,
                want: Err("missing 'name'"),
            },
            Case {
                name: "unknown key is rejected",
                toml: r#"depends_on = [{ name = "otel", requird = false }]"#,
                want: Err("unknown key 'requird'"),
            },
        ];

        for case in cases {
            let parsed: Result<Holder, _> = toml::from_str(case.toml);
            match case.want {
                Ok(want) => {
                    let holder = parsed.unwrap_or_else(|e| panic!("case '{}': {e}", case.name));
                    assert_eq!(holder.depends_on, want, "case '{}'", case.name);
                }
                Err(fragment) => {
                    let err = match parsed {
                        Err(e) => e.to_string(),
                        Ok(_) => panic!("case '{}': expected a parse error", case.name),
                    };
                    assert!(
                        err.contains(fragment),
                        "case '{}': error '{err}' should mention '{fragment}'",
                        case.name
                    );
                }
            }
        }
    }

    #[test]
    fn dependency_round_trips_through_serialization() {
        #[derive(Serialize)]
        struct Out {
            depends_on: Vec<Dependency>,
        }

        let out = Out {
            depends_on: vec![Dependency::required("db"), Dependency::optional("otel")],
        };
        let text = toml::to_string(&out).unwrap();
        let back: Holder = toml::from_str(&text).unwrap();
        assert_eq!(back.depends_on, out.depends_on);
    }

    #[test]
    fn push_dependency_keeps_the_strictest_kind() {
        struct Case {
            name: &'static str,
            existing: Vec<Dependency>,
            incoming: Dependency,
            want: Vec<Dependency>,
        }

        let cases = vec![
            Case {
                name: "new name is appended",
                existing: vec![Dependency::required("db")],
                incoming: Dependency::optional("otel"),
                want: vec![Dependency::required("db"), Dependency::optional("otel")],
            },
            Case {
                name: "required upgrades an existing optional edge",
                existing: vec![Dependency::optional("db")],
                incoming: Dependency::required("db"),
                want: vec![Dependency::required("db")],
            },
            Case {
                name: "optional does not downgrade an existing required edge",
                existing: vec![Dependency::required("db")],
                incoming: Dependency::optional("db"),
                want: vec![Dependency::required("db")],
            },
        ];

        for case in cases {
            let mut list = case.existing;
            push_dependency(&mut list, case.incoming);
            assert_eq!(list, case.want, "case '{}'", case.name);
        }
    }

    #[test]
    fn display_marks_optional_edges() {
        assert_eq!(Dependency::required("db").to_string(), "db");
        assert_eq!(Dependency::optional("db").to_string(), "db (optional)");
    }
}
