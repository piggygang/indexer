-- The Helius webhook transport's durable buffer.
--
-- The Enhanced WebSocket has no `fromSlot`, no cursor and no delivery
-- guarantee, so `ResumeFrom::Slot` is only a floor and every reconnect is a
-- permanent hole. Webhooks move delivery off a socket this process has to stay
-- glued to and onto Helius's own retries — but Helius gives the endpoint about
-- one second to answer and (per its FAQ) retries only three times before the
-- event is lost for good. So receipt and processing are separated: the api
-- service appends here and returns 200, and the ingester's `WebhookInbox`
-- drains at its own pace.
--
-- It is a queue, NOT a decode cache. Raw webhooks deliver `encoding: "json"`
-- instructions — `{"accounts":[0,1,2],"data":"...","programIdIndex":10}` —
-- while `crates/ingest`'s decoder dispatches on `instruction.parsed.type` and
-- reads `programId`/`accounts` as base58 strings. Fed index-form input it
-- returns `Decoded::default()`: no error, no log, just `recorded=0` forever.
-- The drain therefore re-fetches each transaction with `getTransaction`, whose
-- jsonParsed shape the decoder already reads on the recovery path. `body` is
-- kept anyway — it is the forensic record of what Helius actually sent, and the
-- input a future index-form decoder would read instead of re-fetching.
CREATE TABLE webhook_inbox (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    signature    text COLLATE "C" NOT NULL CHECK (is_signature(signature)),
    slot         bigint NOT NULL CHECK (slot >= 0),
    -- The delivery's own `blockTime`. The WS notification carries none, which
    -- is why the live path pays for `getBlockTime` and parks what it cannot
    -- resolve; a webhook does carry one.
    block_time   timestamptz,
    -- `meta.err` was set. The decoder drops failed transactions anyway, so the
    -- drain must never spend a `getTransaction` on one.
    failed       boolean NOT NULL DEFAULT false,
    received_at  timestamptz NOT NULL DEFAULT now(),
    -- Claim lease. A process killed mid-batch leaves rows claimed but not
    -- processed; they become claimable again once the lease expires. That is
    -- at-least-once, which the ingest contract already requires and
    -- `activity::record` already absorbs.
    claimed_at   timestamptz,
    processed_at timestamptz,
    -- Bounded so one signature `getTransaction` never returns cannot be
    -- re-claimed forever — and, worse, pin the watermark forever.
    attempts     smallint NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error   text,
    body         jsonb NOT NULL,
    -- Helius redelivers on any non-2xx and warns that duplicates are expected.
    -- One signature, one row, so `ON CONFLICT DO NOTHING` makes a retry free —
    -- the same discipline `activity` applies one layer down.
    UNIQUE (signature)
);

-- The drain's only hot query: oldest unprocessed first. Partial, so the index
-- is the size of the backlog rather than of the retained history.
CREATE INDEX webhook_inbox_pending ON webhook_inbox (id) WHERE processed_at IS NULL;

-- Pruning, and the `max(processed slot)` half of the watermark.
CREATE INDEX webhook_inbox_processed ON webhook_inbox (processed_at)
    WHERE processed_at IS NOT NULL;

-- `source` is a closed set on both writers, so the new transport has to be
-- admitted before it can write. Expand-only: `NOT VALID` skips the full-table
-- scan under the migration's lock, and VALIDATE then re-reads without blocking
-- writers. Both are strict supersets of the existing constraint, so no stored
-- row can fail them — the split is about lock duration, not about risk.
--
-- Tagging the lane in the row (rather than only in `ingest_state`) is what
-- makes a dual run measurable one event at a time: "did the WebSocket record
-- something the webhook never delivered" is a query, not an inference.
ALTER TABLE activity DROP CONSTRAINT activity_source_check;
ALTER TABLE activity ADD CONSTRAINT activity_source_check
    CHECK (source IN ('backfill', 'live', 'reconcile', 'manual', 'webhook')) NOT VALID;
ALTER TABLE activity VALIDATE CONSTRAINT activity_source_check;

ALTER TABLE ownership_history DROP CONSTRAINT ownership_history_source_check;
ALTER TABLE ownership_history ADD CONSTRAINT ownership_history_source_check
    CHECK (source IN ('backfill', 'live', 'reconcile', 'manual', 'webhook')) NOT VALID;
ALTER TABLE ownership_history VALIDATE CONSTRAINT ownership_history_source_check;
