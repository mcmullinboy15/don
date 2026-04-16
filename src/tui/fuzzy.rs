//! Tiny subsequence-based fuzzy matcher. Used for filter queries and action
//! palette queries — both want "type a few chars, see relevant matches."
//!
//! Matching is case-insensitive. A name matches the query when every query
//! char appears in order inside the name. Results are sorted so tighter
//! matches (characters closer together) come first.
//!
//! Intentionally not using `fuzzy-matcher` or similar — the input set is
//! 5–30 service names, so a hand-rolled matcher is smaller and faster.

/// Return names that match the query, ordered best-first.
pub(crate) fn fuzzy_match(query: &str, names: &[String]) -> Vec<String> {
    if query.is_empty() {
        return names.to_vec();
    }
    let query_lower = query.to_lowercase();
    let mut scored: Vec<(usize, &String)> = names
        .iter()
        .filter_map(|name| {
            subsequence_score(&query_lower, &name.to_lowercase()).map(|s| (s, name))
        })
        .collect();
    // Lower score = tighter match. Stable sort to preserve original order on ties.
    scored.sort_by_key(|(score, _)| *score);
    scored.into_iter().map(|(_, n)| n.clone()).collect()
}

/// Score the subsequence match. `None` if query is not a subsequence.
/// Lower scores indicate characters that appeared closer together.
fn subsequence_score(query: &str, target: &str) -> Option<usize> {
    let mut chars = target.chars().enumerate().peekable();
    let mut score = 0usize;
    let mut last_pos = 0;
    for qc in query.chars() {
        let mut found = false;
        while let Some(&(pos, tc)) = chars.peek() {
            chars.next();
            if tc == qc {
                score += pos.saturating_sub(last_pos);
                last_pos = pos;
                found = true;
                break;
            }
        }
        if !found {
            return None;
        }
    }
    Some(score)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn names(ns: &[&str]) -> Vec<String> {
        ns.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn fuzzy_match_table() {
        struct Case {
            name: &'static str,
            query: &'static str,
            input: Vec<String>,
            want: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "empty query returns all, order preserved",
                query: "",
                input: names(&["api", "db", "worker"]),
                want: vec!["api", "db", "worker"],
            },
            Case {
                name: "substring match",
                query: "api",
                input: names(&["api", "gateway-api", "db", "worker"]),
                want: vec!["api", "gateway-api"],
            },
            Case {
                name: "subsequence match, case insensitive",
                query: "wkr",
                input: names(&["worker", "db", "api"]),
                want: vec!["worker"],
            },
            Case {
                name: "no match drops entry",
                query: "zzz",
                input: names(&["api", "db"]),
                want: vec![],
            },
            Case {
                name: "tighter match ranks higher",
                query: "ab",
                input: names(&["ab", "aXb", "aXXb"]),
                want: vec!["ab", "aXb", "aXXb"],
            },
        ];

        for case in cases {
            let got = fuzzy_match(case.query, &case.input);
            let got_refs: Vec<&str> = got.iter().map(String::as_str).collect();
            assert_eq!(got_refs, case.want, "case: {}", case.name);
        }
    }
}
