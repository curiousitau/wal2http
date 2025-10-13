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

// Helper function for relation lookup
fn get_rel<'a>(
    relations: &'a HashMap<u32, RelationInfo>,
    id: u32,
    ctx: &str,
) -> Result<&'a RelationInfo> {
    relations.get(&id)
        .ok_or_else(|| ReplicationError::parse_with_context(ctx, id.to_string()))
}

// Helper function for building metadata
fn make_meta(pairs: &[(&str, Option<String>)]) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for &(k, ref v) in pairs {
        if let Some(ref v) = v {
            m.insert(k.to_string(), v.clone());
        }
    }
    m
}

// Builder for FormattedEvent
struct EventBuilder {
    ev: super::FormattedEvent,
}

impl EventBuilder {
    fn new(event_type: &str) -> Self {
        Self { ev: super::FormattedEvent {
            event_type: event_type.to_string(),
            transaction_id: None,
            lsn: None,
            timestamp: None,
            schema: None,
            table: None,
            data: serde_json::Value::Object(Default::default()),
            metadata: Default::default(),
        }}
    }

    fn tx(mut self, x: Option<u32>) -> Self { self.ev.transaction_id = x; self }
    fn lsn(mut self, x: Option<u64>) -> Self { self.ev.lsn = x; self }
    fn ts(mut self, x: Option<i64>) -> Self { self.ev.timestamp = x; self }
    fn schema(mut self, s: Option<String>) -> Self { self.ev.schema = s; self }
    fn table(mut self, t: Option<String>) -> Self { self.ev.table = t; self }
    fn data(mut self, d: serde_json::Value) -> Self { self.ev.data = d; self }
    fn meta(mut self, m: HashMap<String, String>) -> Self { self.ev.metadata = m; self }
    fn build(self) -> super::FormattedEvent { self.ev }
}

impl EventFormatter for JsonEventFormatter {
    fn format_message(&self, message: &ReplicationMessage, relations: &HashMap<u32, RelationInfo>) -> Result<super::FormattedEvent> {
        match message {
            ReplicationMessage::Begin { xid, final_lsn, timestamp } => {
                Ok(EventBuilder::new("begin")
                    .tx(Some(*xid))
                    .lsn(Some(*final_lsn))
                    .ts(Some(*timestamp))
                    .data(serde_json::json!({ "xid": xid, "final_lsn": final_lsn, "timestamp": timestamp }))
                    .build())
            }

            ReplicationMessage::Commit { flags, commit_lsn, end_lsn, timestamp } => {
                Ok(EventBuilder::new("commit")
                    .lsn(Some(*end_lsn))
                    .ts(Some(*timestamp))
                    .data(serde_json::json!({ "flags": flags, "commit_lsn": commit_lsn, "end_lsn": end_lsn, "timestamp": timestamp }))
                    .build())
            }

            ReplicationMessage::Insert { relation_id, tuple_data, is_stream, xid } => {
                let rel = get_rel(relations, *relation_id, "Unknown relation ID in INSERT")?;
                let data = self.format_tuple_data(tuple_data, rel)?;
                let meta = make_meta(&[
                    ("is_stream", Some(is_stream.to_string())),
                    ("streaming_xid", xid.map(|x| x.to_string()))
                ]);

                Ok(EventBuilder::new("insert")
                    .tx(*xid)
                    .schema(Some(rel.namespace.clone()))
                    .table(Some(rel.relation_name.clone()))
                    .data(data)
                    .meta(meta)
                    .build())
            }

            ReplicationMessage::Update { relation_id, key_type, old_tuple_data, new_tuple_data, is_stream, xid } => {
                let rel = get_rel(relations, *relation_id, "Unknown relation ID in UPDATE")?;
                let mut data = serde_json::Map::new();
                if let Some(old_data) = old_tuple_data {
                    data.insert("old".to_string(), self.format_tuple_data(old_data, rel)?);
                }
                data.insert("new".to_string(), self.format_tuple_data(new_tuple_data, rel)?);

                let meta = make_meta(&[
                    ("is_stream", Some(is_stream.to_string())),
                    ("key_type", key_type.map(|k| k.to_string())),
                    ("streaming_xid", xid.map(|x| x.to_string()))
                ]);

                Ok(EventBuilder::new("update")
                    .tx(*xid)
                    .schema(Some(rel.namespace.clone()))
                    .table(Some(rel.relation_name.clone()))
                    .data(serde_json::Value::Object(data))
                    .meta(meta)
                    .build())
            }

            ReplicationMessage::Delete { relation_id, key_type, tuple_data, is_stream, xid } => {
                let rel = get_rel(relations, *relation_id, "Unknown relation ID in DELETE")?;
                let data = self.format_tuple_data(tuple_data, rel)?;
                let meta = make_meta(&[
                    ("is_stream", Some(is_stream.to_string())),
                    ("key_type", Some(key_type.to_string())),
                    ("streaming_xid", xid.map(|x| x.to_string()))
                ]);

                Ok(EventBuilder::new("delete")
                    .tx(*xid)
                    .schema(Some(rel.namespace.clone()))
                    .table(Some(rel.relation_name.clone()))
                    .data(data)
                    .meta(meta)
                    .build())
            }

            ReplicationMessage::Truncate { relation_ids, flags, is_stream, xid } => {
                let truncated_tables: Vec<String> = relation_ids
                    .iter()
                    .filter_map(|id| relations.get(id))
                    .map(|rel| format!("{}.{}", rel.namespace, rel.relation_name))
                    .collect();

                let meta = make_meta(&[
                    ("is_stream", Some(is_stream.to_string())),
                    ("flags", Some(flags.to_string())),
                    ("streaming_xid", xid.map(|x| x.to_string()))
                ]);

                Ok(EventBuilder::new("truncate")
                    .tx(*xid)
                    .data(serde_json::json!({
                        "tables": truncated_tables,
                        "flags": flags
                    }))
                    .meta(meta)
                    .build())
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

                Ok(EventBuilder::new("relation")
                    .schema(Some(relation.namespace.clone()))
                    .table(Some(relation.relation_name.clone()))
                    .data(serde_json::json!({
                        "oid": relation.oid,
                        "replica_identity": relation.replica_identity,
                        "column_count": relation.column_count,
                        "columns": columns
                    }))
                    .build())
            }

            ReplicationMessage::StreamStart { xid, first_segment } => {
                let meta = make_meta(&[("first_segment", Some(first_segment.to_string()))]);

                Ok(EventBuilder::new("stream_start")
                    .tx(Some(*xid))
                    .data(serde_json::json!({ "xid": xid, "first_segment": first_segment }))
                    .meta(meta)
                    .build())
            }

            ReplicationMessage::StreamStop => {
                Ok(EventBuilder::new("stream_stop")
                    .data(serde_json::Value::Object(serde_json::Map::new()))
                    .build())
            }

            ReplicationMessage::StreamCommit { xid, flags, commit_lsn, end_lsn, timestamp } => {
                Ok(EventBuilder::new("stream_commit")
                    .tx(Some(*xid))
                    .lsn(Some(*end_lsn))
                    .ts(Some(*timestamp))
                    .data(serde_json::json!({
                        "xid": xid,
                        "flags": flags,
                        "commit_lsn": commit_lsn,
                        "end_lsn": end_lsn,
                        "timestamp": timestamp
                    }))
                    .build())
            }

            ReplicationMessage::StreamAbort { xid, subtransaction_xid } => {
                Ok(EventBuilder::new("stream_abort")
                    .tx(Some(*xid))
                    .data(serde_json::json!({
                        "xid": xid,
                        "subtransaction_xid": subtransaction_xid
                    }))
                    .build())
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