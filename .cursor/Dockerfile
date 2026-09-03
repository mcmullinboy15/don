FROM rust:1-bookworm

# Stable system dependencies for building don (ghostty-vt via Zig, web UI via Node).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        cmake \
        curl \
        git \
        pkg-config \
        xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Ghostty pins Zig exactly; libghostty-vt-sys compiles with `zig build` at build time.
ARG ZIG_VERSION=0.15.2
RUN curl -sSf "https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" -o /tmp/zig.tar.xz \
    && tar -xf /tmp/zig.tar.xz -C /opt \
    && ln -s "/opt/zig-x86_64-linux-${ZIG_VERSION}/zig" /usr/local/bin/zig \
    && rm /tmp/zig.tar.xz

# Node 22 — matches CI and web/package-lock.json.
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Cloud Agents run as ubuntu; match the default image convention.
RUN useradd -m -s /bin/bash ubuntu || true
USER ubuntu
WORKDIR /home/ubuntu
