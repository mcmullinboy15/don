#!/usr/bin/env python3
"""Generate a synthetic don.toml that mirrors a real-world monorepo
shape, for stress-testing the TUI shutdown path with tui_drive.py.

Output:
  <dest>/don.toml          full config
  <dest>/svc/*.sh          one shell script per service

Shape:
  - several non-lazy `infra` services with `hidden = true` (kafka, mongo, …);
    one of them spams ~500 lines on SIGTERM to mimic kafka shutdown chatter
  - non-lazy consumer services depending on infra; several spam in their
    TERM trap (kafka-relay style)
  - many lazy `app-NN` services with `listenfd` proxies, depending on
    the `infra` group (admin/api/customer-portal style)

Usage:
  python3 tools/gen_stress_config.py <dest-dir>
"""
import os, sys, stat

dest_dir = sys.argv[1] if len(sys.argv) > 1 else '.'
os.makedirs(os.path.join(dest_dir, 'svc'), exist_ok=True)

infra = [
    ("fake-kafka", True, True),
    ("fake-mongo", True, False),
    ("fake-postgres", True, False),
    ("fake-search", True, False),
    ("fake-temporal", True, False),
    ("fake-valkey", True, False),
    ("caddy", True, False),
]
consumers = [
    ("kafka-relay", "fake-kafka", True),
    ("mongo-relay", "fake-mongo", True),
    ("change-forwarder", "fake-kafka", True),
    ("notice-relay", "fake-mongo", False),
]
NUM_APPS = 40
START_PORT = 18000

def write_script(path, body):
    with open(path, 'w') as f:
        f.write("#!/bin/sh\n")
        f.write(body + "\n")
    os.chmod(path, 0o755)

# Generate scripts.
for name, _, noisy in infra:
    p = os.path.join(dest_dir, 'svc', f'{name}.sh')
    if noisy:
        body = (
            "trap 'i=1; while [ $i -le 500 ]; do echo \"" + name + " spam $i\"; i=$((i+1)); done; sleep 0.3; exit 0' TERM\n"
            "while true; do sleep 1; done"
        )
    else:
        body = (
            "trap 'echo " + name + " got SIGTERM; sleep 0.3; exit 0' TERM\n"
            "while true; do sleep 1; done"
        )
    write_script(p, body)

for name, dep, noisy in consumers:
    p = os.path.join(dest_dir, 'svc', f'{name}.sh')
    if noisy:
        body = (
            "trap 'i=1; while [ $i -le 200 ]; do echo \"" + name + ": cant flush $i\"; i=$((i+1)); done; exit 0' TERM\n"
            "while true; do sleep 0.05; echo \"" + name + " tick\"; done"
        )
    else:
        body = (
            "trap 'echo " + name + " got SIGTERM; sleep 0.2; exit 0' TERM\n"
            "while true; do sleep 1; done"
        )
    write_script(p, body)

for i in range(NUM_APPS):
    p = os.path.join(dest_dir, 'svc', f'app-{i:02d}.sh')
    write_script(p, "while true; do sleep 1; done")

# Now generate the TOML.
lines = []
lines.append('default_profile = "all"\n\n')
lines.append('[service_groups]\n')
lines.append('infra = [' + ", ".join(f'"{n}"' for n, _, _ in infra) + ']\n\n')

def emit_service(name, hidden=True, deps=None, lazy=False, port=None):
    lines.append(f'[services.{name}]\n')
    lines.append(f'run.cmd = "{os.path.join(dest_dir, "svc", name + ".sh")}"\n')
    if not lazy:
        lines.append('ready.exec.cmd = "true"\n')
    if hidden:
        lines.append('hidden = true\n')
    if lazy:
        lines.append('lazy = true\n')
    if port is not None:
        lines.append(f'proxy = {{ listen = "127.0.0.1:{port}", listenfd = true }}\n')
    if deps:
        lines.append(f'depends_on = {deps}\n')
    lines.append('\n')

active_names = []
for name, hidden, _ in infra:
    emit_service(name, hidden=hidden)
    active_names.append(name)
for name, dep, _ in consumers:
    emit_service(name, deps=[dep])
    active_names.append(name)
for i in range(NUM_APPS):
    name = f"app-{i:02d}"
    emit_service(name, hidden=False, lazy=True, port=START_PORT + i, deps=["infra"])
    active_names.append(name)

# Format deps lists as TOML arrays.
text = "".join(lines).replace("['", '["').replace("']", '"]').replace("',", '",').replace("'", '"')

lines.append('[profiles.all]\n')
lines.append('services = [\n')
for n in active_names:
    text += ""
text2 = text + 'services = [\n' + "".join(f'  "{n}",\n' for n in active_names) + ']\n'
# Restructure: prepend profile header.
text_final = text + '[profiles.all]\nservices = [\n' + "".join(f'  "{n}",\n' for n in active_names) + ']\n'

dest_toml = os.path.join(dest_dir, 'don.toml')
with open(dest_toml, 'w') as f:
    f.write(text_final)
print(f"wrote {dest_toml} with {len(active_names)} services")
