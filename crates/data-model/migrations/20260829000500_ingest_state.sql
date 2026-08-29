-- Durable cursor per live stream = one IngestSource::subscribe() call (one
-- connection carrying every FilterId of the SubscriptionSpec). Written ONLY
-- on SlotCheckpoint (crates/ingest contract); read back as ResumeFrom::Slot
-- (inclusive). The upsert uses GREATEST so a stale writer can never move the
-- cursor backwards; a deliberate rewind goes through ingest_state::reset.
CREATE TABLE ingest_state (
    -- '<IngestSource::name()>:<label>', e.g. 'helius-ws:mainnet'.
    stream              text PRIMARY KEY,
    last_processed_slot bigint NOT NULL CHECK (last_processed_slot >= 0),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- Per-collection backfill cursors: ALG-621 (kind = 'das_assets'), ALG-622
-- (kind = 'activity'), later media/rarity passes. `kind` is an open set —
-- each backfill owns its value; `cursor` is opaque JSON because DAS page
-- cursors and per-asset crawl progress have different shapes.
CREATE TABLE backfill_state (
    collection_id integer NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    kind          text NOT NULL CHECK (kind ~ '^[a-z_]+$'),
    status        text NOT NULL DEFAULT 'idle'
                  CHECK (status IN ('idle', 'running', 'done', 'failed')),
    cursor        jsonb NOT NULL DEFAULT '{}'::jsonb,
    -- Counters for logs/metrics (processed, corrections-per-run, ...).
    progress      jsonb NOT NULL DEFAULT '{}'::jsonb,
    last_error    text,
    started_at    timestamptz,
    finished_at   timestamptz,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (collection_id, kind)
);
