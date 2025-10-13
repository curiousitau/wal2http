#!/bin/bash

# Test script for Hook0EventSink implementation
# This script tests the Hook0EventSink by running the replication server
# with Hook0 configuration and verifying event processing

# Set up environment variables
export slot_name="test_hook0_slot"
export pub_name="test_hook0_pub"

# Set up email configuration for testing
export EMAIL_FROM="test@example.com"
export EMAIL_TO="admin@example.com"
export SMTP_HOST="localhost"
export SMTP_PORT="8082"
export SMTP_USER="test"
export SMTP_PASS="test"

# Start mock servers
echo "Starting mock servers..."
node test_email_server.js &
EMAIL_SERVER_PID=$!
sleep 2

node test_hook0_server.js &
HOOK0_SERVER_PID=$!
sleep 2

# Run the replication checker with Hook0 sink enabled
echo "Starting replication checker with Hook0 sink..."
cd backends/wal2http
cargo run -- \
  --hook0-api-url=http://localhost:8081/events \
  --hook0-application-id=123e4567-e89b-12d3-a456-426614174000 \
  --hook0-api-token=test_api_token \
  user postgres replication database host localhost dbname pg-hasura port 5432 password VTxmG85vh9JpJDSqW1mHYcQlzfmemeT4 &

# Give it time to process some events
echo "Waiting for events to be processed..."
sleep 10

# Check the results
echo "Checking Hook0 API server logs..."
curl http://localhost:8081/events

echo "Checking email server logs..."
curl http://localhost:8082/emails

# Clean up
echo "Test completed, stopping mock servers..."
kill $EMAIL_SERVER_PID
kill $HOOK0_SERVER_PID