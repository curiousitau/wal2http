//! Replication state management
//!
//! Provides state tracking for PostgreSQL logical replication connections,
//! including schema information, LSN positions, and feedback timing.


// Re-export the ReplicationState from protocol messages for convenience
pub use crate::protocol::messages::ReplicationState;



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_state_creation() {
        let state = ReplicationState::new();
        assert_eq!(state.received_lsn, 0);
        assert_eq!(state.applied_lsn, 0);
        assert!(!state.has_received_data());
    }

    #[test]
    fn test_lsn_updates() {
        let mut state = ReplicationState::new();

        // Test received LSN updates
        state.update_lsn(100);
        assert_eq!(state.received_lsn, 100);
        assert!(state.has_received_data());

        // Test that lower LSN doesn't override higher one
        state.update_lsn(50);
        assert_eq!(state.received_lsn, 100);

        // Test applied LSN updates
        state.update_applied_lsn(80);
        assert_eq!(state.applied_lsn, 80);
    }

    #[test]
    fn test_feedback_timing() {
        let state = ReplicationState::new();

        // Should not send feedback immediately
        assert!(!state.should_send_feedback(1));

        // Update feedback time and check again
        state.update_feedback_time();
        assert!(!state.should_send_feedback(1));
    }

    #[test]
    fn test_relation_management() {
        let mut state = ReplicationState::new();

        let relation = RelationInfo {
            oid: 12345,
            namespace: "public".to_string(),
            relation_name: "test_table".to_string(),
            replica_identity: 'd',
            column_count: 2,
            columns: vec![],
        };

        // Add relation
        state.add_relation(relation.clone());

        // Retrieve relation
        let retrieved = state.get_relation(12345);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().relation_name, "test_table");

        // Non-existent relation should return None
        assert!(state.get_relation(99999).is_none());
    }

    #[test]
    fn test_state_reset() {
        let mut state = ReplicationState::new();

        // Add some data
        state.update_lsn(100);
        state.update_applied_lsn(80);
        state.add_relation(RelationInfo {
            oid: 12345,
            namespace: "public".to_string(),
            relation_name: "test_table".to_string(),
            replica_identity: 'd',
            column_count: 1,
            columns: vec![],
        });

        // Verify data exists
        assert!(state.has_received_data());
        assert!(state.get_relation(12345).is_some());

        // Reset state
        state.reset();

        // Verify reset
        assert_eq!(state.received_lsn, 0);
        assert_eq!(state.applied_lsn, 0);
        assert!(!state.has_received_data());
        assert!(state.get_relation(12345).is_none());
    }
}