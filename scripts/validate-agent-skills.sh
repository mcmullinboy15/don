#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
status=0

for skill_dir in "$repo_root"/skills/*; do
  [ -d "$skill_dir" ] || continue

  skill_name="$(basename "$skill_dir")"
  skill_file="$skill_dir/SKILL.md"

  if [ ! -f "$skill_file" ]; then
    echo "error: $skill_name is missing SKILL.md" >&2
    status=1
    continue
  fi

  if ! grep -q '^---$' "$skill_file"; then
    echo "error: $skill_name/SKILL.md is missing YAML frontmatter markers" >&2
    status=1
  fi

  if ! grep -q "^name: $skill_name$" "$skill_file"; then
    echo "error: $skill_name/SKILL.md name must match directory name" >&2
    status=1
  fi

  if ! grep -Eq '^description: .+' "$skill_file"; then
    echo "error: $skill_name/SKILL.md is missing description" >&2
    status=1
  fi
done

exit "$status"
