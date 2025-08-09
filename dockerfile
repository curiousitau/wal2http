FROM rust:1.75 as builder

# Install PostgreSQL development libraries
RUN apt-get update && apt-get install -y libpq-dev

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

# Install PostgreSQL client libraries
RUN apt-get update && apt-get install -y libpq5 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/pg_replica_rs /usr/local/bin/

ENTRYPOINT ["pg_replica_rs"]