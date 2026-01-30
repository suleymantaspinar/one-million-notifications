-- Idempotency table for tracking processed notifications
-- Used to prevent duplicate processing of the same message
CREATE TABLE IF NOT EXISTS processed_notifications (
    notification_id UUID PRIMARY KEY,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Index for efficient cleanup of expired records
CREATE INDEX IF NOT EXISTS idx_processed_notifications_expires_at 
    ON processed_notifications(expires_at);

-- Function to clean up expired idempotency records
-- Can be called periodically via a cron job or scheduled task
CREATE OR REPLACE FUNCTION cleanup_expired_idempotency_records()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM processed_notifications 
    WHERE expires_at < NOW();
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
