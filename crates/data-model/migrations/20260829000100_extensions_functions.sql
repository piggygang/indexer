-- Extensions and helper functions shared by every later migration.

-- btree_gist lets an EXCLUDE constraint combine `asset_id WITH =` with a
-- range `&&` (ownership_history). Trusted contrib extension in PG13+, so the
-- application role may create it.
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Solana pubkey = 32 bytes -> 32..44 base58 chars; signature = 64 bytes ->
-- 86..88 chars. Cheap sanity checks on every address column; bs58 decoding
-- happens in the seeder, not here.
CREATE FUNCTION is_pubkey(v text) RETURNS boolean
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    RETURN v ~ '^[1-9A-HJ-NP-Za-km-z]{32,44}$';

CREATE FUNCTION is_signature(v text) RETURNS boolean
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    RETURN v ~ '^[1-9A-HJ-NP-Za-km-z]{86,88}$';

-- NftSummary.number: the first "#<digits>" run in an asset name ("#7687",
-- "Pig Mud #348" -> 348). Up to 9 digits so it fits int4; NULL when absent.
-- Immutable so it can back a generated column: every writer (backfill, live,
-- manual SQL) gets the same parse.
CREATE FUNCTION token_number(name text) RETURNS integer
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    RETURN (regexp_match(name, '#\s*(\d{1,9})(?!\d)'))[1]::integer;

CREATE FUNCTION set_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END $$;
