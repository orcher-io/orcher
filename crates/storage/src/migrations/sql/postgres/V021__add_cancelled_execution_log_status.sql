-- Add 'cancelled' value to execution_log_status enum for task cancellation support.
-- ALTER TYPE ADD VALUE is transactional in PostgreSQL 12+ and non-destructive.
ALTER TYPE execution_log_status ADD VALUE IF NOT EXISTS 'cancelled';
