#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Keep Rust current (edition 2024) and install lint tooling used by don.toml tasks.
if command -v rustup >/dev/null 2>&1; then
  rustup update stable
  rustup default stable
  rustup component add clippy rustfmt
fi

# Web UI bundle is embedded in the binary but is a build artifact (not committed).
npm --prefix web ci
npm --prefix web run build

# Build the don CLI used by terminals and development workflows.
cargo build --locked
