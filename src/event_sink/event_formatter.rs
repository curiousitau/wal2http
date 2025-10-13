use serde_json::json;

use crate::types::ReplicationMessage;

/// Event formatter to convert replication events to JSON
pub struct EventFormatter;

impl EventFormatter {
    /// Format a replication message as JSON
    pub fn format(message: &ReplicationMessage) -> serde_json::Value {
        match message {
            ReplicationMessage::Begin { xid, .. } => json!({
                "type": "begin",
                "xid": xid,
            }),
            ReplicationMessage::Commit {
                flags,
                commit_lsn,
                end_lsn,
                timestamp,
            } => json!({
                "type": "commit",
                "flags": flags,
                "commit_lsn": commit_lsn,
                "end_lsn": end_lsn,
                "timestamp": timestamp,
            }),
            ReplicationMessage::Relation { relation } => json!({
                "type": "relation",
                "relation": {
                    "oid": relation.oid,
                    "namespace": relation.namespace,
                    "relation_name": relation.relation_name,
                    "replica_identity": relation.replica_identity,
                    "column_count": relation.column_count,
                    "columns": relation.columns.iter().map(|col| json!({
                        "key_flag": col.key_flag,
                        "column_name": col.column_name,
                        "column_type": col.column_type,
                        "atttypmod": col.atttypmod,
                    })).collect::<Vec<_>>(),
                }
            }),
            ReplicationMessage::Insert {
                relation_id,
                tuple_data,
                is_stream,
                xid,
            } => json!({
                "type": "insert",
                "relation_id": relation_id,
                "tuple_data": Self::format_tuple_data(tuple_data),
                "is_stream": is_stream,
                "xid": xid,
            }),
            ReplicationMessage::Update {
                relation_id,
                key_type,
                old_tuple_data,
                new_tuple_data,
                is_stream,
                xid,
            } => json!({
                "type": "update",
                "relation_id": relation_id,
                "key_type": key_type,
                "old_tuple_data": old_tuple_data.as_ref().map(Self::format_tuple_data),
                "new_tuple_data": Self::format_tuple_data(new_tuple_data),
                "is_stream": is_stream,
                "xid": xid,
            }),
            ReplicationMessage::Delete {
                relation_id,
                key_type,
                tuple_data,
                is_stream,
                xid,
            } => json!({
                "type": "delete",
                "relation_id": relation_id,
                "key_type": key_type,
                "tuple_data": Self::format_tuple_data(tuple_data),
                "is_stream": is_stream,
                "xid": xid,
            }),
            ReplicationMessage::Truncate {
                relation_ids,
                flags,
                is_stream,
                xid,
            } => json!({
                "type": "truncate",
                "relation_ids": relation_ids,
                "flags": flags,
                "is_stream": is_stream,
                "xid": xid,
            }),
            ReplicationMessage::StreamStart { xid, first_segment } => json!({
                "type": "stream_start",
                "xid": xid,
                "first_segment": first_segment,
            }),
            ReplicationMessage::StreamStop => json!({
                "type": "stream_stop",
            }),
            ReplicationMessage::StreamCommit {
                xid,
                flags,
                commit_lsn,
                end_lsn,
                timestamp,
            } => json!({
                "type": "stream_commit",
                "xid": xid,
                "flags": flags,
                "commit_lsn": commit_lsn,
                "end_lsn": end_lsn,
                "timestamp": timestamp,
            }),
            ReplicationMessage::StreamAbort {
                xid,
                subtransaction_xid,
            } => json!({
                "type": "stream_abort",
                "xid": xid,
                "subtransaction_xid": subtransaction_xid,
            }),
        }
    }

    /// Format tuple data for JSON serialization
    pub(crate) fn format_tuple_data(tuple_data: &crate::types::TupleData) -> serde_json::Value {
        json!({
            "column_count": tuple_data.column_count,
            "columns": tuple_data.columns.iter().map(|col| json!({
                "data_type": col.data_type,
                "length": col.length,
                "data": col.data,
            })).collect::<Vec<_>>(),
            "processed_length": tuple_data.processed_length,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::types::{ColumnData, TupleData};

    #[test]
    fn test_format_begin_event() {
        let message = ReplicationMessage::Begin {
            final_lsn: 12345,
            timestamp: 1609459200000000,
            xid: 42,
        };

        let json = EventFormatter::format(&message);
        assert_eq!(json["type"], "begin");
        assert_eq!(json["xid"], 42);
    }

    #[test]
    fn test_format_insert_event() {
        let tuple_data = TupleData {
            column_count: 2,
            columns: vec![
                ColumnData {
                    data_type: 't',
                    length: 4,
                    data: "test".to_string(),
                },
                ColumnData {
                    data_type: 'n',
                    length: 0,
                    data: String::new(),
                },
            ],
            processed_length: 12,
        };

        let message = ReplicationMessage::Insert {
            relation_id: 123,
            tuple_data,
            is_stream: false,
            xid: None,
        };

        let json = EventFormatter::format(&message);
        assert_eq!(json["type"], "insert");
        assert_eq!(json["relation_id"], 123);
        assert_eq!(json["tuple_data"]["columns"].as_array().unwrap().len(), 2);
    }
}
