#!/bin/bash

# Test script for EVENT_SINK environment variable

echo "Testing EVENT_SINK environment variable..."

# Test 1: Invalid event sink
echo "Test 1: Invalid event sink"
export EVENT_SINK="invalid"
export DATABASE_URL="postgresql://test:test@localhost/test"
cargo run 2>&1 | grep -q "Event sink must be one of" && echo "✓ Invalid sink rejected" || echo "✗ Invalid sink not rejected"

# Test 2: Valid HTTP event sink but missing HTTP_ENDPOINT_URL
echo "Test 2: HTTP event sink without HTTP_ENDPOINT_URL"
export EVENT_SINK="http"
unset HTTP_ENDPOINT_URL
cargo run 2>&1 | grep -q "Missing HTTP endpoint URL" && echo "✓ HTTP sink requires URL" || echo "✗ HTTP sink missing URL check failed"

# Test 3: Valid Hook0 event sink but missing config
echo "Test 3: Hook0 event sink without config"
export EVENT_SINK="hook0"
unset HOOK0_API_URL
unset HOOK0_APPLICATION_ID
unset HOOK0_API_TOKEN
cargo run 2>&1 | grep -q "Missing Hook0 configuration" && echo "✓ Hook0 sink requires config" || echo "✗ Hook0 sink missing config check failed"

# Test 4: Valid stdout event sink (should work without additional config)
echo "Test 4: STDOUT event sink"
export EVENT_SINK="stdout"
# This will fail at database connection but should pass event sink validation
cargo run 2>&1 | grep -q "Events will be sent to STDOUT" && echo "✓ STDOUT sink initialized" || echo "✗ STDOUT sink initialization failed"

# Test 5: No EVENT_SINK specified (should default to stdout)
echo "Test 5: No event sink specified"
unset EVENT_SINK
cargo run 2>&1 | grep -q "Events will be sent to STDOUT" && echo "✓ Default to STDOUT" || echo "✗ Default to STDOUT failed"

echo "Tests completed."