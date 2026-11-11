//! # Pagination Support
//!
//! Pagination utilities for repository query results.

use serde::{Deserialize, Serialize};

/// Pagination parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pagination {
    /// Page number (1-indexed)
    pub page: u64,
    /// Number of items per page
    pub page_size: u64,
}

impl Pagination {
    /// Create a new pagination with page number and size
    pub fn new(page: u64, page_size: u64) -> Self {
        Self { page, page_size }
    }

    /// Calculate the offset for database queries
    pub fn offset(&self) -> u64 {
        (self.page.saturating_sub(1)) * self.page_size
    }

    /// Get the limit for database queries
    pub fn limit(&self) -> u64 {
        self.page_size
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            page: 1,
            page_size: 20,
        }
    }
}

/// Paginated result containing items and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page<T> {
    /// Items in the current page
    pub items: Vec<T>,
    /// Current page number
    pub page: u64,
    /// Page size
    pub page_size: u64,
    /// Total number of items across all pages
    pub total_items: u64,
    /// Total number of pages
    pub total_pages: u64,
}

impl<T> Page<T> {
    /// Create a new page result
    pub fn new(items: Vec<T>, page: u64, page_size: u64, total_items: u64) -> Self {
        let total_pages = if page_size > 0 {
            total_items.div_ceil(page_size)
        } else {
            0
        };

        Self {
            items,
            page,
            page_size,
            total_items,
            total_pages,
        }
    }

    /// Check if there is a next page
    pub fn has_next(&self) -> bool {
        self.page < self.total_pages
    }

    /// Check if there is a previous page
    pub fn has_prev(&self) -> bool {
        self.page > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagination_offset() {
        let page1 = Pagination::new(1, 10);
        assert_eq!(page1.offset(), 0);

        let page2 = Pagination::new(2, 10);
        assert_eq!(page2.offset(), 10);

        let page3 = Pagination::new(3, 20);
        assert_eq!(page3.offset(), 40);
    }

    #[test]
    fn test_pagination_default() {
        let page = Pagination::default();
        assert_eq!(page.page, 1);
        assert_eq!(page.page_size, 20);
    }

    #[test]
    fn test_page_has_next() {
        let page: Page<String> = Page::new(vec![], 1, 10, 50);
        assert!(page.has_next());
        assert!(!page.has_prev());

        let last_page: Page<String> = Page::new(vec![], 5, 10, 50);
        assert!(!last_page.has_next());
        assert!(last_page.has_prev());
    }

    #[test]
    fn test_page_total_pages() {
        let page: Page<String> = Page::new(vec![], 1, 10, 25);
        assert_eq!(page.total_pages, 3);
    }
}
