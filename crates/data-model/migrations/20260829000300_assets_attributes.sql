-- Assets (one row per NFT) and their normalized attributes.
--
-- THE population predicate for browse, facets and stats-by-attribute is
--   assets.collection_id = $1 AND assets.membership_status = 'member'
-- Burned assets stay in that population (NftSummary.burned lets the UI grey
-- them out; the contract has no burned filter and the Explorer mock counts
-- them) — only `supply`/`holders` exclude them. Every query over the
-- population must apply exactly this predicate so lists and facet counts
-- never disagree.
CREATE TABLE assets (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Public NFT id: mint (token_metadata) or asset id (core).
    address             text COLLATE "C" NOT NULL UNIQUE CHECK (is_pubkey(address)),
    -- The standard is the collection's (a collection cannot mix standards);
    -- NftDetail.standard comes from the join.
    collection_id       integer NOT NULL REFERENCES collections(id),
    -- Core assets can leave a collection (update authority moves them);
    -- reconciliation (ALG-624) flips this instead of deleting history.
    membership_status   text NOT NULL DEFAULT 'member'
                        CHECK (membership_status IN ('member', 'removed')),
    removed_at          timestamptz,
    name                text NOT NULL DEFAULT '',
    number              integer GENERATED ALWAYS AS (token_number(name)) STORED,
    -- On-chain symbol (token_metadata); allowlist validation signal.
    symbol              text,
    -- Off-chain metadata URI as recorded on chain (NftDetail.metadataUri) and
    -- the URI the backfill actually fetched (after the collection's
    -- metadata_uri_template, if any) — a refetch is due when they diverge.
    -- The fetched JSON itself lives in asset_documents to keep this row narrow.
    metadata_uri        text,
    metadata_source_uri text,
    -- Original image URI from the JSON; ALG-621 checks reachability.
    image_uri           text,
    image_status        text NOT NULL DEFAULT 'unknown'
                        CHECK (image_status IN ('unknown', 'ok', 'dead')),
    image_checked_at    timestamptz,
    burned              boolean NOT NULL DEFAULT false,
    -- Observed current owner (DAS backfill / live account updates), always
    -- stamped with the slot of the observation so a stale writer can be
    -- rejected (`owner_slot < new slot` is the writer's guard). DAS
    -- snapshots stamp a conservative lower bound (getSlot before the batch).
    -- NULL when burned or not yet known. ownership_history is the tx-derived
    -- view; reconciliation (ALG-624) diffs the two.
    owner               text COLLATE "C" CHECK (owner IS NULL OR is_pubkey(owner)),
    owner_slot          bigint CHECK (owner_slot IS NULL OR owner_slot >= 0),
    -- Set by a writer that saw an out-of-order event it must not apply to
    -- ownership_history directly; the per-asset rebuild (ALG-622) clears it.
    ownership_dirty     boolean NOT NULL DEFAULT false,
    -- Maintained by the activity trigger; backs sort=-activity.
    last_activity_slot  bigint,
    last_activity_at    timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT assets_burned_has_no_owner CHECK (NOT burned OR owner IS NULL),
    CONSTRAINT assets_owner_has_slot CHECK (owner IS NULL OR owner_slot IS NOT NULL),
    CONSTRAINT assets_removed_pair CHECK ((membership_status = 'removed') = (removed_at IS NOT NULL)),
    CONSTRAINT assets_last_activity_pair
        CHECK ((last_activity_slot IS NULL) = (last_activity_at IS NULL)),
    -- Target of the composite FK on activity, so activity.collection_id can
    -- never disagree with the asset's collection.
    UNIQUE (id, collection_id)
)
-- Room for HOT updates: owner/last_activity/image_status change often while
-- the browse indexes do not.
WITH (fillfactor = 85);
CREATE TRIGGER assets_updated_at BEFORE UPDATE ON assets
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Browse sorts, keyset-paginated on (sort key, id). NULL sort keys are
-- mapped to a sentinel INSIDE the index expression so a keyset cursor never
-- loses NULL rows: ORDER BY coalesce(number, 2147483647) [DESC], id [DESC]
-- and ORDER BY coalesce(last_activity_slot, -1) DESC, id DESC — one index
-- serves both directions via a backward scan.
CREATE INDEX assets_browse_number   ON assets (collection_id, (coalesce(number, 2147483647)), id);
CREATE INDEX assets_browse_name     ON assets (collection_id, name COLLATE "C", id);
CREATE INDEX assets_browse_activity ON assets (collection_id, (coalesce(last_activity_slot, -1)), id);
-- Owner filter + holders (distinct owners per collection); wallet portfolio.
CREATE INDEX assets_collection_owner ON assets (collection_id, owner) WHERE owner IS NOT NULL;
CREATE INDEX assets_owner_collection ON assets (owner, collection_id, id) WHERE owner IS NOT NULL;
-- Supply and burned counts.
CREATE INDEX assets_collection_burned ON assets (collection_id, burned);
-- Rebuild queue.
CREATE INDEX assets_ownership_dirty ON assets (id) WHERE ownership_dirty;

-- Fetched off-chain JSON, one document per asset. Separate from assets so
-- the hot row stays narrow and re-fetches never rewrite it; the original
-- hosts die (shdw-drive), so this is the durable copy.
CREATE TABLE asset_documents (
    asset_id      bigint PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    metadata_json jsonb NOT NULL,
    source_uri    text NOT NULL,
    fetched_at    timestamptz NOT NULL DEFAULT now()
);

-- Attribute dictionary, interned per collection ("Background" in two
-- collections are two trait types — facets are per collection).
CREATE TABLE trait_types (
    id            integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    collection_id integer NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    name          text NOT NULL,
    -- false = still shown on the detail page, excluded from facets. Derived
    -- from collections.facet_exclude on insert and re-synced by the seeder.
    is_facet      boolean NOT NULL DEFAULT true,
    UNIQUE (collection_id, name),
    -- Target of the composite FK on asset_attributes.
    UNIQUE (id, collection_id)
);

CREATE TABLE trait_values (
    id            integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trait_type_id integer NOT NULL REFERENCES trait_types(id) ON DELETE CASCADE,
    -- Exact, case-sensitive (API contract).
    value         text NOT NULL,
    UNIQUE (trait_type_id, value),
    -- Target of the composite FK below.
    UNIQUE (id, trait_type_id)
);
ALTER TABLE trait_values ALTER COLUMN trait_type_id SET STATISTICS 1000;

-- (asset, trait_type, value). The PK is on the value, so a collection that
-- repeats a trait type with different values is representable. collection_id
-- is denormalized so two composite FKs can prove the asset and the trait
-- type belong to the same collection — an attribute can never leak into
-- another collection's facets.
CREATE TABLE asset_attributes (
    asset_id       bigint   NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    collection_id  integer  NOT NULL,
    trait_type_id  integer  NOT NULL,
    trait_value_id integer  NOT NULL,
    -- Order in the source metadata (detail page).
    position       smallint NOT NULL DEFAULT 0,
    PRIMARY KEY (asset_id, trait_value_id),
    FOREIGN KEY (asset_id, collection_id)
        REFERENCES assets(id, collection_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (trait_type_id, collection_id)
        REFERENCES trait_types(id, collection_id) ON DELETE CASCADE,
    -- trait_type_id is denormalized for facet grouping; this FK makes it
    -- impossible to disagree with the value's own type.
    FOREIGN KEY (trait_value_id, trait_type_id)
        REFERENCES trait_values(id, trait_type_id) ON DELETE CASCADE
);
-- Filter path: value -> assets (OR within a type = ANY(ids); AND across
-- types = intersect).
CREATE INDEX asset_attributes_by_value ON asset_attributes (trait_value_id, asset_id);
-- A per-asset-unique trait (thousands of singleton values) would otherwise
-- push the real facet values out of the MCV list and mislead the planner.
-- Backfills must ANALYZE asset_attributes when they finish.
ALTER TABLE asset_attributes ALTER COLUMN trait_value_id SET STATISTICS 1000;
ALTER TABLE asset_attributes ALTER COLUMN trait_type_id  SET STATISTICS 1000;

-- Unfiltered facet counts over the browse population (member assets,
-- burned included) and facet trait types only. Predicates on collection_id
-- push down through the GROUP BY. The filtered (disjunctive) counts are a
-- parameterized query in indexer_data_model::facets.
CREATE VIEW facet_counts AS
SELECT tt.collection_id,
       tt.id   AS trait_type_id,
       tt.name AS trait_type,
       tv.id   AS trait_value_id,
       tv.value,
       count(*)::integer AS count
FROM trait_types tt
JOIN trait_values     tv ON tv.trait_type_id = tt.id
JOIN asset_attributes aa ON aa.trait_value_id = tv.id
JOIN assets           a  ON a.id = aa.asset_id AND a.collection_id = tt.collection_id
                        AND a.membership_status = 'member'
WHERE tt.is_facet
GROUP BY tt.collection_id, tt.id, tt.name, tv.id, tv.value;
