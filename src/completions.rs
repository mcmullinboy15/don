//! Shell completion support.
//!
//! Two moving parts:
//!
//! 1. `don __complete <kind>` — a hidden subcommand (see `main.rs`) that
//!    prints names from the config, one per line. Completion scripts shell
//!    out to this for positional arg candidates.
//! 2. [`emit_script`] — generates the shell's completion script. For shells
//!    where we have a postlude (bash/zsh/fish), we append dynamic hooks on
//!    top of the stock `clap_complete::generate` output. Other shells get
//!    static completions only.

use crate::config::Config;
use clap_complete::Shell;
use std::path::Path;
use std::str::FromStr;

/// Which category of names a completion script is asking for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompleteKind {
    /// Service names only.
    Services,
    /// Task names only.
    Tasks,
    /// Services + tasks merged (for `logs`, `attach`).
    Items,
    /// Profile names only.
    Profiles,
}

impl FromStr for CompleteKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "services" => Ok(Self::Services),
            "tasks" => Ok(Self::Tasks),
            "items" => Ok(Self::Items),
            "profiles" => Ok(Self::Profiles),
            other => Err(format!("unknown completion kind '{other}'")),
        }
    }
}

/// Load the config at `path` and return the names matching `kind`, sorted.
///
/// Returns an empty vector when the config is missing or invalid — completion
/// must stay silent on errors so the user's shell never prints parse messages
/// mid tab-press.
pub fn list_names(kind: CompleteKind, config_path: &Path) -> Vec<String> {
    let Ok(config) = Config::from_file(config_path) else {
        return Vec::new();
    };
    collect_names(&config, kind)
}

fn collect_names(config: &Config, kind: CompleteKind) -> Vec<String> {
    let mut names: Vec<String> = match kind {
        CompleteKind::Services => config.services.keys().cloned().collect(),
        CompleteKind::Tasks => config.tasks.keys().cloned().collect(),
        CompleteKind::Items => config
            .services
            .keys()
            .chain(config.tasks.keys())
            .cloned()
            .collect(),
        CompleteKind::Profiles => config.profiles.keys().cloned().collect(),
    };
    names.sort();
    names
}

/// Generate the completion script for `shell`, written to `writer`.
///
/// For bash/zsh/fish, the stock `clap_complete` output is followed by a
/// postlude that delegates positional args on name-taking subcommands to
/// `don __complete`. Other shells get static completions only — their
/// positional name args won't tab-complete dynamically.
pub fn emit_script<W: std::io::Write, T: clap::Parser>(
    shell: Shell,
    bin_name: &str,
    writer: &mut W,
) -> std::io::Result<()> {
    let mut cmd = T::command();
    clap_complete::generate(shell, &mut cmd, bin_name, writer);
    if let Some(postlude) = postlude_for(shell) {
        writer.write_all(postlude.as_bytes())?;
    }
    Ok(())
}

fn postlude_for(shell: Shell) -> Option<&'static str> {
    match shell {
        Shell::Bash => Some(BASH_POSTLUDE),
        Shell::Zsh => Some(ZSH_POSTLUDE),
        Shell::Fish => Some(FISH_POSTLUDE),
        _ => None,
    }
}

/// Wraps the clap-generated `_don` completer with a preflight that emits
/// service/task/item names for positional args. Falls through to `_don` for
/// flag names and non-positional contexts.
const BASH_POSTLUDE: &str = r#"
# --- dynamic completion postlude (see `don completions bash`) ---
_don_dynamic_kind() {
    local i=1 subcmd=""
    while [ $i -lt $COMP_CWORD ]; do
        local w="${COMP_WORDS[$i]}"
        case "$w" in
            -c|--config) i=$((i+2)); continue ;;
            -*) i=$((i+1)); continue ;;
        esac
        subcmd="$w"
        break
    done
    case "$subcmd" in
        start|stop|restart) echo services ;;
        run) echo tasks ;;
        logs|attach) echo items ;;
    esac
}

_don_dynamic() {
    local kind
    kind="$(_don_dynamic_kind)"
    [ -z "$kind" ] && return 1
    local cur="${COMP_WORDS[$COMP_CWORD]}"
    local prev="${COMP_WORDS[$((COMP_CWORD-1))]}"
    # Skip completion when the previous word takes a value or current is a flag.
    case "$prev" in
        -c|--config|-p|--profile|-l|--last) return 1 ;;
    esac
    [[ "$cur" == -* ]] && return 1
    local names
    names="$(command don __complete "$kind" 2>/dev/null)"
    [ -z "$names" ] && return 1
    COMPREPLY=( $(compgen -W "$names" -- "$cur") )
    return 0
}

_don_with_dynamic() {
    if _don_dynamic; then return 0; fi
    _don "$@"
}

# Override the binding `clap_complete` set up above.
complete -F _don_with_dynamic -o nosort -o bashdefault -o default don
"#;

/// Zsh postlude: defines a dispatcher that calls `don __complete` for
/// subcommand-specific positional args, then falls back to `_don`.
///
/// Relies on the standard completion globals `$words` (the command line) and
/// `$CURRENT` (1-based index of the word being completed). Must be defined
/// without re-declaring `words` as local or the outer values get shadowed.
const ZSH_POSTLUDE: &str = r#"
# --- dynamic completion postlude (see `don completions zsh`) ---
_don_dynamic_kind() {
    local i skip=0
    # words[1] is `don` itself, so start scanning at 2. Stop before CURRENT
    # so the word currently being typed doesn't count as the subcommand.
    for (( i=2; i<CURRENT; i++ )); do
        local w="${words[i]}"
        if (( skip )); then skip=0; continue; fi
        case "$w" in
            -c|--config) skip=1 ;;
            -*) : ;;
            *) print -- "$w"; return ;;
        esac
    done
}

_don_with_dynamic() {
    local subcmd kind
    subcmd="$(_don_dynamic_kind)"
    case "$subcmd" in
        start|stop|restart) kind=services ;;
        run) kind=tasks ;;
        logs|attach) kind=items ;;
        *) kind="" ;;
    esac
    if [[ -n "$kind" && "${words[CURRENT]}" != -* ]]; then
        local -a names
        names=("${(@f)$(command don __complete "$kind" 2>/dev/null)}")
        if (( ${#names} > 0 )); then
            compadd -- "${names[@]}"
            return
        fi
    fi
    _don "$@"
}

compdef _don_with_dynamic don
"#;

/// Fish postlude: adds per-subcommand `complete` rules that invoke
/// `don __complete`. Fish's condition machinery is simple enough that we
/// don't need a shared helper — each rule stands alone.
const FISH_POSTLUDE: &str = r#"
# --- dynamic completion postlude (see `don completions fish`) ---
complete -c don -n '__fish_seen_subcommand_from start stop restart' \
    -f -a '(command don __complete services 2>/dev/null)'
complete -c don -n '__fish_seen_subcommand_from run' \
    -f -a '(command don __complete tasks 2>/dev/null)'
complete -c don -n '__fish_seen_subcommand_from logs attach' \
    -f -a '(command don __complete items 2>/dev/null)'
"#;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn make_config(services: &[&str], tasks: &[&str], profiles: &[&str]) -> Config {
        // Build a Config by TOML parse so we don't have to enumerate fields.
        let mut toml = String::new();
        for s in services {
            toml.push_str(&format!("[services.{s}]\nrun.cmd = \"true\"\n"));
        }
        for t in tasks {
            toml.push_str(&format!("[tasks.{t}]\ncmd = \"true\"\n"));
        }
        for p in profiles {
            toml.push_str(&format!("[profiles.{p}]\n"));
        }
        let _ = &toml; // hand off
        toml::from_str(&toml).unwrap()
    }

    struct CollectCase {
        name: &'static str,
        kind: CompleteKind,
        services: &'static [&'static str],
        tasks: &'static [&'static str],
        profiles: &'static [&'static str],
        want: &'static [&'static str],
    }

    #[test]
    fn collect_names_table() {
        let cases = [
            CollectCase {
                name: "services only",
                kind: CompleteKind::Services,
                services: &["api", "worker"],
                tasks: &["migrate"],
                profiles: &["dev"],
                want: &["api", "worker"],
            },
            CollectCase {
                name: "tasks only",
                kind: CompleteKind::Tasks,
                services: &["api"],
                tasks: &["migrate", "seed"],
                profiles: &[],
                want: &["migrate", "seed"],
            },
            CollectCase {
                name: "items merges services and tasks",
                kind: CompleteKind::Items,
                services: &["api"],
                tasks: &["migrate"],
                profiles: &[],
                want: &["api", "migrate"],
            },
            CollectCase {
                name: "profiles only",
                kind: CompleteKind::Profiles,
                services: &["api"],
                tasks: &[],
                profiles: &["dev", "prod"],
                want: &["dev", "prod"],
            },
            CollectCase {
                name: "empty returns empty",
                kind: CompleteKind::Services,
                services: &[],
                tasks: &[],
                profiles: &[],
                want: &[],
            },
        ];

        for case in cases {
            let config = make_config(case.services, case.tasks, case.profiles);
            let got = collect_names(&config, case.kind);
            let want: Vec<String> = case.want.iter().map(|s| s.to_string()).collect();
            assert_eq!(got, want, "{}: got {:?} want {:?}", case.name, got, want);
        }
    }

    #[test]
    fn kind_from_str_rejects_garbage() {
        assert!(CompleteKind::from_str("services").is_ok());
        assert!(CompleteKind::from_str("nope").is_err());
    }

    #[test]
    fn list_names_returns_empty_on_missing_config() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("nowhere.toml");
        assert!(list_names(CompleteKind::Services, &missing).is_empty());
    }

    // Mirror the clap derive just enough to exercise emit_script without
    // pulling in the real `Cli` from main.rs.
    #[derive(clap::Parser)]
    struct MiniCli {
        #[command(subcommand)]
        _cmd: MiniSub,
    }
    #[derive(clap::Subcommand)]
    enum MiniSub {
        Start,
        Stop,
    }

    #[test]
    fn emit_script_appends_postlude_for_known_shells() {
        let mut out = Vec::new();
        emit_script::<_, MiniCli>(Shell::Bash, "don", &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("_don_with_dynamic"), "bash postlude missing");

        let mut out = Vec::new();
        emit_script::<_, MiniCli>(Shell::Fish, "don", &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("__fish_seen_subcommand_from"), "fish postlude missing");

        let mut out = Vec::new();
        emit_script::<_, MiniCli>(Shell::PowerShell, "don", &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        // PowerShell has no postlude; just sanity-check we emitted something.
        assert!(!text.is_empty());
        assert!(!text.contains("_don_dynamic"), "postlude leaked into powershell");
    }
}
