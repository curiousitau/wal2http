#!/bin/bash

# Run the replication checker with HTTP sink enabled
export HTTP_ENDPOINT_URL=http://localhost:8080/events
export RUST_LOG=debug
sops exec-env local.sops.env "cargo run"