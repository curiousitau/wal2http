# Multi-stage build for wal2http Rust project
# Build stage: Install dependencies and build the project
FROM rust:1.89-trixie AS builder

# Set working directory
WORKDIR /build

# Copy Cargo.toml and lockfile for dependency caching
COPY Cargo.toml Cargo.lock ./

# Install PostgreSQL development libraries
RUN apt-get update && apt-get install -y \
	libpq-dev libclang-dev \
	&& rm -rf /var/lib/apt/lists/*

RUN mkdir src \
	&& echo "// dummy file" > src/lib.rs \
	&& cargo build --release

# Copy source code
COPY src/ ./src/

# Build the project
RUN cargo build --release

# Final stage: Create minimal production image
FROM debian:trixie-slim

# Set working directory
WORKDIR /usr/local

# Install runtime PostgreSQL libraries
RUN apt-get update && apt-get install -y \
	libpq5 \
	&& rm -rf /var/lib/apt/lists/*

# Copy built binary from build stage
COPY --from=builder /build/target/release/wal2http /usr/local/bin/wal2http

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/wal2http"]

