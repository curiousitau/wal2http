//! Event formatting for PostgreSQL logical replication messages
//! Converts internal replication messages to standardized event formats

use crate::errors::ReplicationError;
use crate::types::*;
use std::collections::HashMap;
use tracing::warn;

type Result<T> = std::result::Result<T, ReplicationError>;

/// Event formatter trait for converting replication messages to standardized formats
pub trait EventFormatter {
    /// Format a replication message into a standardized event
    fn format_message(&self, message: &ReplicationMessage, relations: &HashMap<u32, RelationInfo>) -> Result<super::FormattedEvent>;
}

/// Default JSON event formatter
pub struct JsonEventFormatter {
    include_null_values: bool,
    max_field_length: Option<usize>,
}

impl JsonEventFormatter {
    /// Create a new JSON event formatter
    pub fn new() -> Self {
        Self {
            include_null_values: false,
            max_field_length: Some(1000),
        }
    }

    /// Configure whether to include NULL values in the output
    pub fn include_null_values(mut self, include: bool) -> Self {
        self.include_null_values = include;
        self
    }

    /// Set maximum field length (None for unlimited)
    pub fn max_field_length(mut self, max_len: Option<usize>) -> Self {
        self.max_field_length = max_len;
        self
    }
}

impl Default for JsonEventFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl EventFormatter for JsonEventFormatter {
    fn format_message(&self, message: &ReplicationMessage, relations: &HashMap<u32, RelationInfo>) -> Result<super::FormattedEvent> {
        match message {
            ReplicationMessage::Begin { xid, final_lsn, timestamp } => {
                Ok(super::FormattedEvent {
                    event_type: "begin".to_string(),
                    transaction_id: Some(*xid),
                    lsn: Some(*final_lsn),
                    timestamp: Some(*timestamp),
                    schema: None,
                    table: None,
                    data: serde_json::json!({
                        "xid": xid,
                        "final_lsn": final_lsn,
                        "timestamp": timestamp
                    }),
                    metadata: HashMap::new(),
                })
            }

            ReplicationMessage::Commit { flags, commit_lsn, end_lsn, timestamp } => {
                Ok(super::FormattedEvent {
                    event_type: "commit".to_string(),
                    transaction_id: None,
                    lsn: Some(*end_lsn),
                    timestamp: Some(*timestamp),
                    schema: None,
                    table: None,
                    data: serde_json::json!({
                        "flags": flags,
                        "commit_lsn": commit_lsn,
                        "end_lsn": end_lsn,
                        "timestamp": timestamp
                    }),
                    metadata: HashMap::new(),
                })
            }

            ReplicationMessage::Insert { relation_id, tuple_data, is_stream, xid } => {
                let relation = relations.get(relation_id)
                    .ok_or_else(|| crate::errors::ReplicationError::parse_with_context(
                        "Unknown relation ID in INSERT",
                        format!("Relation ID: {}", relation_id)
                    ))?;

                let data = self.format_tuple_data(tuple_data, relation)?;
                let mut metadata = HashMap::new();
                metadata.insert("is_stream".to_string(), is_stream.to_string());
                if let Some(xid) = xid {
                    metadata.insert("streaming_xid".to_string(), xid.to_string());
                }

                Ok(super::FormattedEvent {
                    event_type: "insert".to_string(),
                    transaction_id: *xid,
                    lsn: None,
                    timestamp: None,
                    schema: Some(relation.namespace.clone()),
                    table: Some(relation.relation_name.clone()),
                    data,
                    metadata,
                })
            }

            ReplicationMessage::Update { relation_id, key_type, old_tuple_data, new_tuple_data, is_stream, xid } => {
                let relation = relations.get(relation_id)
                    .ok_or_else(|| crate::errors::ReplicationError::parse_with_context(
                        "Unknown relation ID in UPDATE",
                        format!("Relation ID: {}", relation_id)
                    ))?;

                let mut data = serde_json::Map::new();
                if let Some(old_data) = old_tuple_data {
                    data.insert("old".to_string(), self.format_tuple_data(old_data, relation)?);
                }
                data.insert("new".to_string(), self.format_tuple_data(new_tuple_data, relation)?);

                let mut metadata = HashMap::new();
                metadata.insert("is_stream".to_string(), is_stream.to_string());
                if let Some(key_type) = key_type {
                    metadata.insert("key_type".to_string(), key_type.to_string());
                }
                if let Some(xid) = xid {
                    metadata.insert("streaming_xid".to_string(), xid.to_string());
                }

                Ok(super::FormattedEvent {
                    event_type: "update".to_string(),
                    transaction_id: *xid,
                    lsn: None,
                    timestamp: None,
                    schema: Some(relation.namespace.clone()),
                    table: Some(relation.relation_name.clone()),
                    data: serde_json::Value::Object(data),
                    metadata,
                })
            }

            ReplicationMessage::Delete { relation_id, key_type, tuple_data, is_stream, xid } => {
                let relation = relations.get(relation_id)
                    .ok_or_else(|| crate::errors::ReplicationError::parse_with_context(
                        "Unknown relation ID in DELETE",
                        format!("Relation ID: {}", relation_id)
                    ))?;

                let data = self.format_tuple_data(tuple_data, relation)?;
                let mut metadata = HashMap::new();
                metadata.insert("is_stream".to_string(), is_stream.to_string());
                metadata.insert("key_type".to_string(), key_type.to_string());
                if let Some(xid) = xid {
                    metadata.insert("streaming_xid".to_string(), xid.to_string());
                }

                Ok(super::FormattedEvent {
                    event_type: "delete".to_string(),
                    transaction_id: *xid,
                    lsn: None,
                    timestamp: None,
                    schema: Some(relation.namespace.clone()),
                    table: Some(relation.relation_name.clone()),
                    data,
                    metadata,
                })
            }

            ReplicationMessage::Truncate { relation_ids, flags, is_stream, xid } => {
                let truncated_tables: Vec<String> = relation_ids
                    .iter()
                    .filter_map(|id| relations.get(id))
                    .map(|rel| format!("{}.{}", rel.namespace, rel.relation_name))
                    .collect();

                let mut metadata = HashMap::new();
                metadata.insert("is_stream".to_string(), is_stream.to_string());
                metadata.insert("flags".to_string(), flags.to_string());
                if let Some(xid) = xid {
                    metadata.insert("streaming_xid".to_string(), xid.to_string());
                }

                Ok(super::FormattedEvent {
                    event_type: "truncate".to_string(),
                    transaction_id: *xid,
                    lsn: None,
                    timestamp: None,
                    schema: None,
                    table: None,
                    data: serde_json::json!({
                        "tables": truncated_tables,
                        "flags": flags
                    }),
                    metadata,
                })
            }

            ReplicationMessage::Relation { relation } => {
                let columns: Vec<serde_json::Value> = relation.columns
                    .iter()
                    .map(|col| serde_json::json!({
                        "name": col.column_name,
                        "type": col.column_type,
                        "key_flag": col.key_flag,
                        "atttypmod": col.atttypmod
                    }))
                    .collect();

                Ok(super::FormattedEvent {
                    event_type: "relation".to_string(),
                    transaction_id: None,
                    lsn: None,
                    timestamp: None,
                    schema: Some(relation.namespace.clone()),
                    table: Some(relation.relation_name.clone()),
                    data: serde_json::json!({
                        "oid": relation.oid,
                        "replica_identity": relation.replica_identity,
                        "column_count": relation.column_count,
                        "columns": columns
                    }),
                    metadata: HashMap::new(),
                })
            }

            ReplicationMessage::StreamStart { xid, first_segment } => {
                let mut metadata = HashMap::new();
                metadata.insert("first_segment".to_string(), first_segment.to_string());

                Ok(super::FormattedEvent {
                    event_type: "stream_start".to_string(),
                    transaction_id: Some(*xid),
                    lsn: None,
                    timestamp: None,
                    schema: None,
                    table: None,
                    data: serde_json::json!({
                        "xid": xid,
                        "first_segment": first_segment
                    }),
                    metadata,
                })
            }

            ReplicationMessage::StreamStop => {
                Ok(super::FormattedEvent {
                    event_type: "stream_stop".to_string(),
                    transaction_id: None,
                    lsn: None,
                    timestamp: None,
                    schema: None,
                    table: None,
                    data: serde_json::Value::Object(serde_json::Map::new()),
                    metadata: HashMap::new(),
                })
            }

            ReplicationMessage::StreamCommit { xid, flags, commit_lsn, end_lsn, timestamp } => {
                Ok(super::FormattedEvent {
                    event_type: "stream_commit".to_string(),
                    transaction_id: Some(*xid),
                    lsn: Some(*end_lsn),
                    timestamp: Some(*timestamp),
                    schema: None,
                    table: None,
                    data: serde_json::json!({
                        "xid": xid,
                        "flags": flags,
                        "commit_lsn": commit_lsn,
                        "end_lsn": end_lsn,
                        "timestamp": timestamp
                    }),
                    metadata: HashMap::new(),
                })
            }

            ReplicationMessage::StreamAbort { xid, subtransaction_xid } => {
                Ok(super::FormattedEvent {
                    event_type: "stream_abort".to_string(),
                    transaction_id: Some(*xid),
                    lsn: None,
                    timestamp: None,
                    schema: None,
                    table: None,
                    data: serde_json::json!({
                        "xid": xid,
                        "subtransaction_xid": subtransaction_xid
                    }),
                    metadata: HashMap::new(),
                })
            }
        }
    }
}

impl JsonEventFormatter {
    /// Format tuple data into a JSON object
    fn format_tuple_data(&self, tuple_data: &TupleData, relation: &RelationInfo) -> Result<serde_json::Value> {
        let mut fields = serde_json::Map::new();

        for (i, column_data) in tuple_data.columns.iter().enumerate() {
            if i >= relation.columns.len() {
                warn!("Column index {} exceeds relation column count {}", i, relation.columns.len());
                continue;
            }

            let column_name = &relation.columns[i].column_name;

            // Skip NULL values if configured to do so
            if column_data.data_type == crate::parser::COLUMN_TYPE_NULL && !self.include_null_values {
                continue;
            }

            let value = match column_data.data_type {
                crate::parser::COLUMN_TYPE_NULL => serde_json::Value::Null,
                crate::parser::COLUMN_TYPE_UNCHANGED_TOAST => {
                    serde_json::Value::String("<TOASTED>".to_string())
                }
                crate::parser::COLUMN_TYPE_TEXT => {
                    let mut value = column_data.data.clone();

                    // Truncate if necessary
                    if let Some(max_len) = self.max_field_length {
                        if value.len() > max_len {
                            value.truncate(max_len);
                            value.push_str("... (truncated)");
                        }
                    }

                    serde_json::Value::String(value)
                }
                _ => {
                    warn!("Unknown column data type: {}", column_data.data_type);
                    serde_json::Value::String("<UNKNOWN>".to_string())
                }
            };

            fields.insert(column_name.clone(), value);
        }

        Ok(serde_json::Value::Object(fields))
    }
}

/// Webhook event formatter - optimized for webhook payloads
pub struct WebhookEventFormatter {
    json_formatter: JsonEventFormatter,
}

impl WebhookEventFormatter {
    /// Create a new webhook event formatter
    pub fn new() -> Self {
        Self {
            json_formatter: JsonEventFormatter::new()
                .include_null_values(false)
                .max_field_length(Some(500)),
        }
    }
}

impl Default for WebhookEventFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl EventFormatter for WebhookEventFormatter {
    fn format_message(&self, message: &ReplicationMessage, relations: &HashMap<u32, RelationInfo>) -> Result<super::FormattedEvent> {
        let mut event = self.json_formatter.format_message(message, relations)?;

        // Add webhook-specific metadata
        event.metadata.insert("format".to_string(), "webhook".to_string());
        event.metadata.insert("version".to_string(), "1.0".to_string());

        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_json_formatter_basic() {
        let formatter = JsonEventFormatter::new();
        let relations = HashMap::new();

        // Test BEGIN message formatting
        let begin_msg = ReplicationMessage::Begin {
            xid: 12345,
            final_lsn: 1000,
            timestamp: 1640995200000000, // 2022-01-01 00:00:00 UTC
        };

        let event = formatter.format_message(&begin_msg, &relations).unwrap();
        assert_eq!(event.event_type, "begin");
        assert_eq!(event.transaction_id, Some(12345));
        assert_eq!(event.lsn, Some(1000));
    }

    #[test]
    fn test_webhook_formatter() {
        let formatter = WebhookEventFormatter::new();
        let relations = HashMap::new();

        let begin_msg = ReplicationMessage::Begin {
            xid: 12345,
            final_lsn: 1000,
            timestamp: 1640995200000000,
        };

        let event = formatter.format_message(&begin_msg, &relations).unwrap();
        assert_eq!(event.event_type, "begin");
        assert_eq!(event.metadata.get("format"), Some(&"webhook".to_string()));
        assert_eq!(event.metadata.get("version"), Some(&"1.0".to_string()));
    }
}