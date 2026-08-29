-- Per-asset on-chain events. Idempotency key: (asset_id, signature, seq).
--   * one signature can touch several assets           -> asset_id in the key
--   * one asset can have several events in one tx      -> seq = ordinal of
--     this asset's events within the tx, in instruction order (0 normally)
-- asset_id leads the key so reclassification (DELETE by asset + signature)
-- and the timeline index share locality.
--
-- Writer contract (ALG-622/623), stated once: every ownership-mutating
-- transaction locks the asset (SELECT ... FOR UPDATE), inserts the activity
-- row(s) with ON CONFLICT DO NOTHING ... RETURNING id, and applies owner /
-- ownership_history changes ONLY when the insert returned a row — at-least-
-- once redelivery (inclusive slot resume) then does nothing twice. An event
-- older than the asset's open interval / owner_slot is stored but NOT
-- applied: set assets.ownership_dirty and let the per-asset rebuild (delete
-- the asset's intervals, re-derive from activity ordered by slot, seq)
-- restore the history. Reclassification = DELETE the (asset, signature)
-- rows + re-insert, then rebuild.
-- block_time: getBlockTime(slot) (cached per slot); account updates carry
-- no timestamp, so their synthetic openers resolve the same way. A signature
-- whose block_time cannot be resolved stays unclassified.
CREATE TABLE activity (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset_id       bigint  NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    -- Denormalized: activity24h/7d stats and "latest Core mints" strips.
    -- The composite FK keeps it equal to the asset's collection.
    collection_id  integer NOT NULL REFERENCES collections(id),
    signature      text COLLATE "C" NOT NULL CHECK (is_signature(signature)),
    seq            smallint NOT NULL DEFAULT 0 CHECK (seq >= 0),
    slot           bigint NOT NULL CHECK (slot >= 0),
    block_time     timestamptz NOT NULL,
    -- The API serves mint/transfer/sale/burn; the rest exist so ALG-622 can
    -- store what it classifies without a migration.
    kind           text NOT NULL
                   CHECK (kind IN ('mint', 'transfer', 'sale', 'burn', 'stake', 'unstake', 'other')),
    from_owner     text COLLATE "C" CHECK (from_owner IS NULL OR is_pubkey(from_owner)),
    to_owner       text COLLATE "C" CHECK (to_owner   IS NULL OR is_pubkey(to_owner)),
    price_lamports bigint CHECK (price_lamports IS NULL OR price_lamports >= 0),
    -- Open set, e.g. 'Magic Eden' | 'Tensor' | 'Solanart' | 'Alpha Art'.
    marketplace    text,
    -- Classifier extras (program ids, currency mint, ...); never load-bearing.
    details        jsonb,
    source         text NOT NULL CHECK (source IN ('backfill', 'live', 'reconcile', 'manual')),
    created_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (asset_id, signature, seq),
    FOREIGN KEY (asset_id, collection_id)
        REFERENCES assets(id, collection_id) ON DELETE CASCADE ON UPDATE CASCADE,
    -- The contract's nullability rules, enforced. A transfer/sale always
    -- has a receiver; the sender may be unknown (escrow-era marketplaces).
    CONSTRAINT activity_mint_shape
        CHECK (kind <> 'mint' OR (from_owner IS NULL AND to_owner IS NOT NULL)),
    CONSTRAINT activity_burn_shape
        CHECK (kind <> 'burn' OR to_owner IS NULL),
    CONSTRAINT activity_transfer_shape
        CHECK (kind NOT IN ('transfer', 'sale') OR to_owner IS NOT NULL),
    CONSTRAINT activity_sale_has_price
        CHECK (kind <> 'sale' OR price_lamports IS NOT NULL),
    CONSTRAINT activity_price_only_sale
        CHECK (kind = 'sale' OR (price_lamports IS NULL AND marketplace IS NULL))
);
-- /nfts/{id}/activity, keyset newest first.
CREATE INDEX activity_asset_timeline       ON activity (asset_id, slot DESC, id DESC);
-- activity24h / activity7d.
CREATE INDEX activity_collection_time      ON activity (collection_id, block_time DESC);
-- Latest Core mints strip.
CREATE INDEX activity_collection_kind_slot ON activity (collection_id, kind, slot DESC);

-- assets.last_activity_* = max(activity.slot). Statement-level with a
-- transition table so a large backfill batch touches each asset once.
-- Monotonic on inserts only; a reclassification that deletes rows recomputes
-- explicitly (ALG-622).
CREATE FUNCTION activity_touch_assets() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    UPDATE assets a
       SET last_activity_slot = n.slot,
           last_activity_at   = n.block_time
      FROM (SELECT DISTINCT ON (asset_id) asset_id, slot, block_time
              FROM inserted
             ORDER BY asset_id, slot DESC, id DESC) n
     WHERE a.id = n.asset_id
       AND (a.last_activity_slot IS NULL OR a.last_activity_slot < n.slot);
    RETURN NULL;
END $$;
CREATE TRIGGER activity_touch_assets
    AFTER INSERT ON activity
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT EXECUTE FUNCTION activity_touch_assets();

-- Raw per-asset signature list (ALG-622): the archival crawl's durable
-- output, so reclassification never re-fetches. classified_at NULL = pending,
-- which makes the crawl resumable per asset from this table alone. Budget:
-- 2021-era pigs can carry 100-200 signatures each (listings, escrow moves)
-- => a few million rows, ~1 GB with the index; keep it lean.
CREATE TABLE asset_signatures (
    asset_id      bigint NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    signature     text COLLATE "C" NOT NULL CHECK (is_signature(signature)),
    slot          bigint NOT NULL CHECK (slot >= 0),
    block_time    timestamptz,
    -- The transaction errored on chain (getSignaturesForAddress.err).
    failed        boolean NOT NULL DEFAULT false,
    fetched_at    timestamptz NOT NULL DEFAULT now(),
    classified_at timestamptz,
    PRIMARY KEY (asset_id, signature)
);
CREATE INDEX asset_signatures_order   ON asset_signatures (asset_id, slot);
CREATE INDEX asset_signatures_pending ON asset_signatures (asset_id) WHERE classified_at IS NULL;

-- Ownership intervals derived from activity (ALG-622), appended by the live
-- pipeline (ALG-623). Invariants enforced here:
--   * intervals of one asset never overlap, hence at most ONE open interval
--     per asset (an open interval is the unbounded range [from, inf))
--   * same-slot hand-offs are legal: [100,100) is the empty range
--   * to_slot / to_ts are set together; to_slot >= from_slot
--   * an event opens at most one interval and closes at most one
-- The EXCLUDE is DEFERRABLE so a writer may open the new interval and close
-- the old one in either order inside one transaction — which also means a
-- violation surfaces at COMMIT, not at the offending statement. "Exactly one
-- open interval" is deliberately not a constraint: burned assets and assets
-- without history have none — that half is the integrity view below.
-- Deleting an activity row never deletes history (SET NULL); rebuilds are
-- explicit. GiST inserts cost several times a btree insert and deferred
-- checks queue until commit: backfill in per-asset transactions.
CREATE TABLE ownership_history (
    id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset_id  bigint NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    owner     text COLLATE "C" NOT NULL CHECK (is_pubkey(owner)),
    from_slot bigint NOT NULL CHECK (from_slot >= 0),
    from_ts   timestamptz NOT NULL,
    to_slot   bigint,
    to_ts     timestamptz,
    -- NULL = synthetic opener (crawl gap, DAS-seeded interval).
    opened_by bigint REFERENCES activity(id) ON DELETE SET NULL,
    -- The burn or the next transfer/sale.
    closed_by bigint REFERENCES activity(id) ON DELETE SET NULL,
    source    text NOT NULL DEFAULT 'backfill'
              CHECK (source IN ('backfill', 'live', 'reconcile', 'manual')),
    CONSTRAINT ownership_to_pair CHECK ((to_slot IS NULL) = (to_ts IS NULL)),
    CONSTRAINT ownership_ordered CHECK (to_slot IS NULL OR to_slot >= from_slot),
    CONSTRAINT ownership_no_overlap
        EXCLUDE USING gist (asset_id WITH =, int8range(from_slot, to_slot, '[)') WITH &&)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (opened_by),
    UNIQUE (closed_by)
);
-- /nfts/{id}/owners, newest first.
CREATE INDEX ownership_history_asset ON ownership_history (asset_id, from_slot DESC, id DESC);
CREATE INDEX ownership_history_open  ON ownership_history (asset_id) WHERE to_slot IS NULL;

-- Collection stats for the API (short-TTL cached there). supply/holders
-- count live members; activity counts events of the collection.
CREATE VIEW collection_stats AS
SELECT c.id AS collection_id,
       (SELECT count(*)              FROM assets a
         WHERE a.collection_id = c.id AND a.membership_status = 'member'
           AND NOT a.burned)::integer                                       AS supply,
       (SELECT count(DISTINCT owner) FROM assets a
         WHERE a.collection_id = c.id AND a.membership_status = 'member'
           AND a.owner IS NOT NULL)::integer                                AS holders,
       (SELECT count(*) FROM activity x
         WHERE x.collection_id = c.id
           AND x.block_time >= now() - interval '24 hours')::integer         AS activity_24h,
       (SELECT count(*) FROM activity x
         WHERE x.collection_id = c.id
           AND x.block_time >= now() - interval '7 days')::integer           AS activity_7d
FROM collections c;

-- Integrity views: what reconciliation (ALG-624) diffs and logs. Empty =
-- healthy. Observed owner (assets.owner) vs derived owner (open interval),
-- for assets that have any history at all.
CREATE VIEW integrity_owner_mismatch AS
SELECT a.id AS asset_id, a.address, a.burned,
       a.owner AS observed_owner, h.owner AS derived_owner
FROM assets a
LEFT JOIN ownership_history h ON h.asset_id = a.id AND h.to_slot IS NULL
WHERE EXISTS (SELECT 1 FROM ownership_history x WHERE x.asset_id = a.id)
  AND a.owner IS DISTINCT FROM h.owner;

-- Asset filed under an allowlist collection but absent from its allowlist.
CREATE VIEW integrity_allowlist_violation AS
SELECT a.id AS asset_id, a.address, c.slug
FROM assets a
JOIN collections c ON c.id = a.collection_id
WHERE c.membership_rule = 'tm_allowlist'
  AND NOT EXISTS (SELECT 1 FROM collection_mints m
                   WHERE m.collection_id = c.id AND m.mint = a.address);

-- Allowlist asset whose on-chain symbol disagrees with the registry. The
-- allowlist is authoritative; a mismatch is surfaced, never acted on.
CREATE VIEW integrity_symbol_mismatch AS
SELECT a.id AS asset_id, a.address, c.slug, a.symbol AS asset_symbol, c.symbol AS expected_symbol
FROM assets a
JOIN collections c ON c.id = a.collection_id
WHERE c.membership_rule = 'tm_allowlist'
  AND c.symbol IS NOT NULL
  AND a.symbol IS NOT NULL
  AND a.symbol <> c.symbol;
