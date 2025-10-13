# Event Sink Configuration

This document describes the `EVENT_SINK` environment variable that allows you to specify which event sink to use for processing PostgreSQL replication events.

## Overview

The wal2http service supports three types of event sinks:

1. **HTTP** - Sends events to a generic HTTP endpoint
2. **Hook0** - Sends events to a Hook0 event management service
3. **STDOUT** - Outputs events to standard output (useful for development/testing)

## Environment Variable

- **Name**: `EVENT_SINK`
- **Type**: String (optional)
- **Default**: `stdout`
- **Valid Values**: `http`, `hook0`, `stdout` (case-insensitive)

## Configuration Examples

### HTTP Event Sink

```bash
export EVENT_SINK=http
export HTTP_ENDPOINT_URL=https://your-webhook-endpoint.com/events
export DATABASE_URL=postgresql://user:password@localhost/db
```

### Hook0 Event Sink

```bash
export EVENT_SINK=hook0
export HOOK0_API_URL=https://your-hook0-instance.com
export HOOK0_APPLICATION_ID=your-app-uuid
export HOOK0_API_TOKEN=your-api-token
export DATABASE_URL=postgresql://user:password@localhost/db
```

### STDOUT Event Sink (Default)

```bash
export DATABASE_URL=postgresql://user:password@localhost/db
# Or explicitly:
export EVENT_SINK=stdout
```

## Service-Specific Requirements

### HTTP Event Sink
- **Required**: `HTTP_ENDPOINT_URL` must be set and point to a valid HTTP/HTTPS endpoint
- **Format**: Must start with `http://` or `https://`

### Hook0 Event Sink
- **Required**: All of the following must be set:
  - `HOOK0_API_URL`: Hook0 instance URL
  - `HOOK0_APPLICATION_ID`: Valid UUID for the application
  - `HOOK0_API_TOKEN`: API authentication token

### STDOUT Event Sink
- **Required**: None (this is the fallback event sink)

## Behavior

1. **Validation**: The `EVENT_SINK` value is validated at startup. Invalid values will cause the service to fail with an error message.

2. **Configuration Check**: If a specific event sink is requested, the corresponding required configuration is validated. Missing required configuration will cause startup to fail.

3. **Fallback**: If `EVENT_SINK` is not specified, the service defaults to `stdout`.

4. **Case Insensitivity**: Event sink names are case-insensitive (`HTTP`, `http`, and `Http` are all equivalent).

## Error Scenarios

### Invalid Event Sink Name
```
Error: Event sink must be one of: 'http', 'hook0', or 'stdout'
```

### Missing HTTP Configuration
```
Error: HTTP event sink specified but missing HTTP_ENDPOINT_URL
```

### Missing Hook0 Configuration
```
Error: Hook0 event sink specified but missing required configuration (api_url, application_id, or api_token)
```

## Migration

Previously, the service would automatically choose between Hook0 and HTTP based on which environment variables were set. With this change, you must explicitly specify the event sink using `EVENT_SINK`.

**Old behavior**:
- If Hook0 variables were set → Use Hook0
- Else if HTTP endpoint was set → Use HTTP
- Else → Use STDOUT

**New behavior**:
- If `EVENT_SINK=http` → Use HTTP (requires `HTTP_ENDPOINT_URL`)
- If `EVENT_SINK=hook0` → Use Hook0 (requires all Hook0 variables)
- If `EVENT_SINK=stdout` or not set → Use STDOUT

This change ensures that only one event sink can be configured at a time, making the configuration more explicit and preventing conflicts.