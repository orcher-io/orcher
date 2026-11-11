//! Chronicle query builder.

use sqlx::{Postgres, QueryBuilder};

use super::types::{ListWorkflowsRequest, OrderBy, PageCursor};

/// Query builder for chronicle queries
pub struct ChronicleQueryBuilder<'a> {
    request: &'a ListWorkflowsRequest,
    cursor: Option<PageCursor>,
}

impl<'a> ChronicleQueryBuilder<'a> {
    /// Create a new query builder
    pub fn new(request: &'a ListWorkflowsRequest) -> Self {
        let cursor = request
            .page_token
            .as_ref()
            .and_then(|token| PageCursor::decode(token).ok());

        Self { request, cursor }
    }

    /// Build the main SELECT query
    pub fn build_query(&self) -> QueryBuilder<'a, Postgres> {
        let mut query = QueryBuilder::new(
            "SELECT workflow_id, workflow_name, namespace, status, \
             started_at, completed_at, started_by, parent_workflow_id, \
             labels, tags, last_error \
             FROM workflow_state_projection WHERE ",
        );

        // Always filter by namespace
        query.push("namespace = ");
        query.push_bind(&self.request.namespace);

        // Add filters
        self.add_status_filter(&mut query);
        self.add_workflow_name_filter(&mut query);
        self.add_started_by_filter(&mut query);
        self.add_parent_filter(&mut query);
        self.add_time_range_filters(&mut query);
        self.add_labels_filter(&mut query);
        self.add_tags_filter(&mut query);

        // Add cursor-based pagination
        self.add_cursor_filter(&mut query);

        // Add ordering
        self.add_order_by(&mut query);

        // Add limit
        query.push(" LIMIT ");
        query.push_bind(self.request.effective_page_size());

        query
    }

    /// Build count query (for total_count)
    pub fn build_count_query(&self) -> QueryBuilder<'a, Postgres> {
        let mut query =
            QueryBuilder::new("SELECT COUNT(*) as count FROM workflow_state_projection WHERE ");

        query.push("namespace = ");
        query.push_bind(&self.request.namespace);

        self.add_status_filter(&mut query);
        self.add_workflow_name_filter(&mut query);
        self.add_started_by_filter(&mut query);
        self.add_parent_filter(&mut query);
        self.add_time_range_filters(&mut query);
        self.add_labels_filter(&mut query);
        self.add_tags_filter(&mut query);

        query
    }

    fn add_status_filter(&self, query: &mut QueryBuilder<'a, Postgres>) {
        if let Some(status) = &self.request.status {
            query.push(" AND status = ");
            query.push_bind(status);
        }
    }

    fn add_workflow_name_filter(&self, query: &mut QueryBuilder<'a, Postgres>) {
        if let Some(name) = &self.request.workflow_name {
            query.push(" AND workflow_name = ");
            query.push_bind(name);
        } else if let Some(prefix) = &self.request.workflow_name_prefix {
            query.push(" AND workflow_name LIKE ");
            query.push_bind(format!("{}%", prefix));
        }
    }

    fn add_started_by_filter(&self, query: &mut QueryBuilder<'a, Postgres>) {
        if let Some(started_by) = &self.request.started_by {
            query.push(" AND started_by = ");
            query.push_bind(started_by);
        }
    }

    fn add_parent_filter(&self, query: &mut QueryBuilder<'a, Postgres>) {
        if let Some(parent_id) = &self.request.parent_workflow_id {
            query.push(" AND parent_workflow_id = ");
            query.push_bind(parent_id);
        }
    }

    fn add_time_range_filters(&self, query: &mut QueryBuilder<'a, Postgres>) {
        if let Some(after) = &self.request.started_after {
            query.push(" AND started_at >= ");
            query.push_bind(after);
        }

        if let Some(before) = &self.request.started_before {
            query.push(" AND started_at <= ");
            query.push_bind(before);
        }

        if let Some(after) = &self.request.completed_after {
            query.push(" AND completed_at >= ");
            query.push_bind(after);
        }

        if let Some(before) = &self.request.completed_before {
            query.push(" AND completed_at <= ");
            query.push_bind(before);
        }
    }

    fn add_labels_filter(&self, query: &mut QueryBuilder<'a, Postgres>) {
        if let Some(attrs) = &self.request.labels {
            if !attrs.is_empty() {
                // JSONB containment query (@>)
                let json_filter = serde_json::to_value(attrs).unwrap();
                query.push(" AND labels @> ");
                query.push_bind(json_filter);
            }
        }
    }

    fn add_tags_filter(&self, query: &mut QueryBuilder<'a, Postgres>) {
        if let Some(tags) = &self.request.tags {
            if !tags.is_empty() {
                // Array contains all tags (@>)
                query.push(" AND tags @> ");
                query.push_bind(tags);
            }
        }
    }

    fn add_cursor_filter(&self, query: &mut QueryBuilder<'a, Postgres>) {
        if let Some(cursor) = &self.cursor {
            // Validate order consistency
            let current_order = self.request.effective_order_by();
            if cursor.order_by != current_order {
                tracing::warn!(
                    "Cursor order_by mismatch: cursor={:?}, request={:?}",
                    cursor.order_by,
                    current_order
                );
                return;
            }

            // Cursor-based pagination for efficient large result sets
            match cursor.order_by {
                OrderBy::StartTimeDesc => {
                    query.push(" AND (started_at, workflow_id) < (");
                    query.push_bind(cursor.last_timestamp);
                    query.push(", ");
                    query.push_bind(cursor.last_workflow_id);
                    query.push(")");
                }
                OrderBy::StartTimeAsc => {
                    query.push(" AND (started_at, workflow_id) > (");
                    query.push_bind(cursor.last_timestamp);
                    query.push(", ");
                    query.push_bind(cursor.last_workflow_id);
                    query.push(")");
                }
                OrderBy::CompletedTimeDesc => {
                    query.push(" AND completed_at IS NOT NULL AND (completed_at, workflow_id) < (");
                    query.push_bind(cursor.last_timestamp);
                    query.push(", ");
                    query.push_bind(cursor.last_workflow_id);
                    query.push(")");
                }
                OrderBy::WorkflowNameAsc => {
                    // Name-based ordering, use started_at as tiebreaker
                    query.push(" AND (started_at, workflow_id) < (");
                    query.push_bind(cursor.last_timestamp);
                    query.push(", ");
                    query.push_bind(cursor.last_workflow_id);
                    query.push(")");
                }
            }
        }
    }

    fn add_order_by(&self, query: &mut QueryBuilder<'a, Postgres>) {
        query.push(" ORDER BY ");
        query.push(self.request.effective_order_by().to_sql());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::time::Timestamptz;
    use uuid::Uuid;

    #[test]
    fn test_basic_query_structure() {
        let request = ListWorkflowsRequest::new("test-ns".to_string());
        let builder = ChronicleQueryBuilder::new(&request);
        let query = builder.build_query();

        let sql = query.sql();
        assert!(sql.contains("SELECT workflow_id"));
        assert!(sql.contains("FROM workflow_state_projection"));
        assert!(sql.contains("namespace = "));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT"));
    }

    #[test]
    fn test_status_filter() {
        let mut request = ListWorkflowsRequest::new("test-ns".to_string());
        request.status = Some(super::super::types::WorkflowStatus::Running);

        let builder = ChronicleQueryBuilder::new(&request);
        let query = builder.build_query();

        assert!(query.sql().contains("status ="));
    }

    #[test]
    fn test_workflow_name_filter() {
        let mut request = ListWorkflowsRequest::new("test-ns".to_string());
        request.workflow_name = Some("my-workflow".to_string());

        let builder = ChronicleQueryBuilder::new(&request);
        let query = builder.build_query();

        assert!(query.sql().contains("workflow_name ="));
    }

    #[test]
    fn test_workflow_name_prefix_filter() {
        let mut request = ListWorkflowsRequest::new("test-ns".to_string());
        request.workflow_name_prefix = Some("order-".to_string());

        let builder = ChronicleQueryBuilder::new(&request);
        let query = builder.build_query();

        assert!(query.sql().contains("workflow_name LIKE"));
    }

    #[test]
    fn test_time_range_filters() {
        let now = Timestamptz::now();
        let mut request = ListWorkflowsRequest::new("test-ns".to_string());
        request.started_after = Some(now);
        request.started_before = Some(now);

        let builder = ChronicleQueryBuilder::new(&request);
        let query = builder.build_query();

        let sql = query.sql();
        assert!(sql.contains("started_at >="));
        assert!(sql.contains("started_at <="));
    }

    #[test]
    fn test_parent_filter() {
        let parent_id = Uuid::new_v4();
        let mut request = ListWorkflowsRequest::new("test-ns".to_string());
        request.parent_workflow_id = Some(parent_id);

        let builder = ChronicleQueryBuilder::new(&request);
        let query = builder.build_query();

        assert!(query.sql().contains("parent_workflow_id ="));
    }

    #[test]
    fn test_count_query() {
        let request = ListWorkflowsRequest::new("test-ns".to_string());
        let builder = ChronicleQueryBuilder::new(&request);
        let query = builder.build_count_query();

        let sql = query.sql();
        assert!(sql.contains("SELECT COUNT(*)"));
        assert!(sql.contains("FROM workflow_state_projection"));
        assert!(!sql.contains("LIMIT"));
        assert!(!sql.contains("ORDER BY"));
    }

    #[test]
    fn test_cursor_pagination() {
        let now = Timestamptz::now();
        let id = Uuid::new_v4();
        let cursor = PageCursor::new(now, id, OrderBy::StartTimeDesc);
        let encoded = cursor.encode().unwrap();

        let mut request = ListWorkflowsRequest::new("test-ns".to_string());
        request.page_token = Some(encoded);

        let builder = ChronicleQueryBuilder::new(&request);
        let query = builder.build_query();

        let sql = query.sql();
        assert!(sql.contains("started_at, workflow_id"));
    }
}
