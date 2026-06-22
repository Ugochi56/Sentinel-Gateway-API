-- ============================================================
-- Sentinel Gateway — Supabase Schema
-- Run this in the Supabase SQL editor once.
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ----------------------------------------------------------
-- 1. Registered Endpoints
--    One row per client registration. Maps to a unique
--    /hooks/{id} proxy URL.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS endpoints (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_api_key     TEXT        NOT NULL,
    destination_url  TEXT        NOT NULL,
    label            TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_endpoints_user_api_key
    ON endpoints(user_api_key);


-- ----------------------------------------------------------
-- 2. Webhook Events (queue + permanent log)
--    raw_body stores the exact bytes received so HMAC
--    signatures remain verifiable. payload is best-effort
--    parsed JSON for display only.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS webhook_events (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id         UUID        NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    user_api_key        TEXT        NOT NULL,

    -- Deduplication: provider event IDs (X-GitHub-Delivery, Stripe event id, etc.)
    -- NULL when the provider sends no identifying header/field.
    provider_event_id   TEXT,

    -- Immutable raw body — never reserialised, preserves HMAC signatures.
    raw_body            TEXT        NOT NULL,
    -- Original provider headers forwarded to the destination.
    headers             JSONB       NOT NULL DEFAULT '{}',
    -- Best-effort parsed JSON for /v1/logs display. May be NULL for non-JSON payloads.
    payload             JSONB,

    status              TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'delivered', 'failed_permanently')),

    retry_count         INTEGER     NOT NULL DEFAULT 0,
    next_retry_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Prevents duplicate delivery when a provider retries the same event.
    CONSTRAINT unique_provider_event UNIQUE (endpoint_id, provider_event_id)
);

-- Worker's primary polling index: pending events due for delivery.
CREATE INDEX IF NOT EXISTS idx_events_worker_poll
    ON webhook_events(status, next_retry_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_events_endpoint_id
    ON webhook_events(endpoint_id);

CREATE INDEX IF NOT EXISTS idx_events_user_api_key
    ON webhook_events(user_api_key);
