-- infra/sql/core_schema.sql
-- Core database schema for the Reality Creation Profile Engine

-- users table
CREATE TABLE users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), -- Assuming pgcrypto or similar for uuid generation
  tier text NOT NULL DEFAULT 'free',
  created_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE users IS 'Stores user account information and tier.';
COMMENT ON COLUMN users.id IS 'Unique identifier for the user.';
COMMENT ON COLUMN users.tier IS 'User subscription tier (e.g., free, premium).';
COMMENT ON COLUMN users.created_at IS 'Timestamp when the user account was created.';


-- log_entries table (intended for TimescaleDB hypertable conversion)
CREATE TABLE log_entries (
  id bigserial PRIMARY KEY,
  user_id uuid NULL REFERENCES users(id) ON DELETE SET NULL, -- Allow anonymous events or keep logs if user deleted
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  algo_version text NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE log_entries IS 'Event log for user actions and system events. Intended to be a TimescaleDB hypertable.';
COMMENT ON COLUMN log_entries.id IS 'Sequential identifier for the log entry.';
COMMENT ON COLUMN log_entries.user_id IS 'Identifier of the user associated with the event (if applicable).';
COMMENT ON COLUMN log_entries.event_type IS 'Type of event logged (e.g., assessment_completed, user_registered).';
COMMENT ON COLUMN log_entries.payload IS 'JSONB data containing event-specific details.';
COMMENT ON COLUMN log_entries.algo_version IS 'Version of the algorithm used, if relevant to the event (e.g., for assessment results).';
COMMENT ON COLUMN log_entries.created_at IS 'Timestamp when the event occurred.';

-- Index for common queries
CREATE INDEX idx_log_entries_user_id ON log_entries(user_id);
CREATE INDEX idx_log_entries_event_type ON log_entries(event_type);
CREATE INDEX idx_log_entries_created_at ON log_entries(created_at DESC); -- TimescaleDB uses this

-- Convert to hypertable (requires TimescaleDB extension enabled)
-- This command should be run after the table is created and the extension is available.
-- SELECT create_hypertable('log_entries', 'created_at', if_not_exists => TRUE);