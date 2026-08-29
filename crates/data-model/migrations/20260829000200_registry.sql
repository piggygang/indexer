-- Collections registry. Rows come ONLY from config/collections.toml via
-- `indexer-admin seed`; no address is ever written in Rust or SQL.
--
-- Membership is DERIVED from the columns (membership_rule), never from code:
--   core_collection : standard='core',           address = Core CollectionV1
--   tm_collection   : standard='token_metadata', address = certified collection mint
--   tm_allowlist    : standard='token_metadata', address NULL,
--                     verified_creator (+symbol) and rows in collection_mints
--   NULL            : disabled placeholder (unknown standard/address);
--                     such a row cannot be enabled.
CREATE TABLE collections (
    id               integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slug             text NOT NULL UNIQUE
                     CHECK (slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$' AND length(slug) <= 64),
    name             text NOT NULL CHECK (length(name) BETWEEN 1 AND 128),
    -- Same enum as the API contract. NULL only while disabled.
    standard         text CHECK (standard IN ('token_metadata', 'core')),
    address          text COLLATE "C" CHECK (address IS NULL OR is_pubkey(address)),
    -- Token Metadata without a certified collection (candy-machine era):
    -- creators[0] with verified = true, plus the on-chain symbol.
    verified_creator text COLLATE "C" CHECK (verified_creator IS NULL OR is_pubkey(verified_creator)),
    -- Informational / validation signal for backfill and reconciliation.
    update_authority text COLLATE "C" CHECK (update_authority IS NULL OR is_pubkey(update_authority)),
    symbol           text CHECK (symbol IS NULL OR length(symbol) BETWEEN 1 AND 10),
    image_url        text,
    -- Config-driven override for the off-chain metadata location, applied by
    -- the backfill when set: '{mint}' is replaced by the asset's mint/asset
    -- id. Needed because on-chain URIs point at dead hosts (shdw-drive) and
    -- the re-hosted files are mint-keyed. NULL = use the on-chain URI.
    metadata_uri_template text
                     CHECK (metadata_uri_template IS NULL
                            OR (metadata_uri_template LIKE 'https://%'
                                AND position('{mint}' IN metadata_uri_template) > 0)),
    -- Trait types stored on assets but never faceted (per-asset-unique
    -- "Name" in Piggy Girl Gang). Applied to trait_types.is_facet.
    facet_exclude    text[] NOT NULL DEFAULT '{}',
    enabled          boolean NOT NULL DEFAULT false,
    membership_rule  text GENERATED ALWAYS AS (
                         CASE
                             WHEN standard = 'core'           AND address IS NOT NULL          THEN 'core_collection'
                             WHEN standard = 'token_metadata' AND address IS NOT NULL          THEN 'tm_collection'
                             WHEN standard = 'token_metadata' AND verified_creator IS NOT NULL THEN 'tm_allowlist'
                         END) STORED,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    -- creator / symbol / update authority are Token Metadata concepts.
    -- (IS NOT DISTINCT FROM: a NULL standard must not make the CHECK pass.)
    CONSTRAINT collections_tm_only_fields
        CHECK (standard IS NOT DISTINCT FROM 'token_metadata'
               OR (verified_creator IS NULL AND symbol IS NULL AND update_authority IS NULL)),
    -- An ENABLED row must resolve to a membership rule — the same predicate
    -- as membership_rule, coalesced so a NULL standard cannot slip through.
    CONSTRAINT collections_enabled_resolvable
        CHECK (NOT enabled
               OR coalesce(standard = 'core'           AND address IS NOT NULL, false)
               OR coalesce(standard = 'token_metadata' AND (address IS NOT NULL OR verified_creator IS NOT NULL), false))
);
CREATE TRIGGER collections_updated_at BEFORE UPDATE ON collections
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Closed-set membership for tm_allowlist collections. A mint belongs to
-- exactly one collection. "allowlist rule => non-empty list" cannot be a
-- CHECK; the seeder enforces it.
CREATE TABLE collection_mints (
    mint          text COLLATE "C" PRIMARY KEY CHECK (is_pubkey(mint)),
    collection_id integer NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX collection_mints_collection ON collection_mints (collection_id);

-- Fungible token registry: registry only, no balances.
CREATE TABLE tokens (
    mint       text COLLATE "C" PRIMARY KEY CHECK (is_pubkey(mint)),
    symbol     text NOT NULL CHECK (length(symbol) BETWEEN 1 AND 10),
    name       text NOT NULL CHECK (length(name) BETWEEN 1 AND 128),
    decimals   smallint NOT NULL CHECK (decimals BETWEEN 0 AND 255),
    logo_uri   text,
    enabled    boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER tokens_updated_at BEFORE UPDATE ON tokens
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
