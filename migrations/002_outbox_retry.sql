-- Add retry_count column to outbox_events table for tracking retry attempts
ALTER TABLE outbox_events 
    ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;

-- Update the check constraint to include 'published' status (alias for 'sent')
-- Note: 'sent' is kept for backward compatibility, 'published' can be used interchangeably
ALTER TABLE outbox_events 
    DROP CONSTRAINT IF EXISTS outbox_events_status_check;

ALTER TABLE outbox_events 
    ADD CONSTRAINT outbox_events_status_check 
    CHECK (status IN ('ready', 'sent', 'failed'));

-- Index for efficient querying of events that need processing
CREATE INDEX IF NOT EXISTS idx_outbox_events_ready_retry 
    ON outbox_events(status, retry_count, created_at) 
    WHERE status = 'ready';
