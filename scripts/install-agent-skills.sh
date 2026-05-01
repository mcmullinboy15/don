#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/install-agent-skills.sh [--target claude|codex|both] [--force]

Installs repo skills into local agent skill directories:
  Claude Code: ~/.claude/skills
  Codex:       ${CODEX_HOME:-$HOME/.codex}/skills
USAGE
}

target="both"
force="false"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --target)
      if [ "$#" -lt 2 ]; then
        echo "error: --target requires claude, codex, or both" >&2
        exit 2
      fi
      target="$2"
      shift 2
      ;;
    --force)
      force="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$target" in
  claude|codex|both) ;;
  *)
    echo "error: --target must be claude, codex, or both" >&2
    exit 2
    ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
skill_root="$repo_root/skills"

if [ ! -d "$skill_root" ]; then
  echo "error: missing skills directory: $skill_root" >&2
  exit 1
fi

install_skills() {
  local dest_root="$1"

  mkdir -p "$dest_root"

  for source_dir in "$skill_root"/*; do
    [ -d "$source_dir" ] || continue

    local skill_name
    local dest_dir
    skill_name="$(basename "$source_dir")"
    dest_dir="$dest_root/$skill_name"

    if [ ! -f "$source_dir/SKILL.md" ]; then
      echo "error: missing skill entrypoint: $source_dir/SKILL.md" >&2
      exit 1
    fi

    if [ -e "$dest_dir" ]; then
      if [ "$force" != "true" ]; then
        echo "error: $dest_dir already exists; pass --force to replace it" >&2
        exit 1
      fi
      rm -rf "$dest_dir"
    fi

    cp -R "$source_dir" "$dest_dir"
    echo "installed $skill_name -> $dest_dir"
  done
}

if [ "$target" = "claude" ] || [ "$target" = "both" ]; then
  install_skills "$HOME/.claude/skills"
fi

if [ "$target" = "codex" ] || [ "$target" = "both" ]; then
  codex_home="${CODEX_HOME:-$HOME/.codex}"
  install_skills "$codex_home/skills"
fi

echo "restart the target agent to pick up newly installed skills"
