-- The program that owns a Metaplex Core collection.
--
-- The decoder reads a Core transfer, burn or mint straight from the
-- instruction (`crates/ingest/src/decode.rs`), and the account layout alone
-- does not say who issued it: any program can be invoked with our collection
-- at account 1 and `[14, 0]` as its data, and believing that would move an
-- owner on a stranger's say-so. Classification is therefore gated on the
-- instruction's `programId` being this column. NULL = not configured: every
-- Core instruction stays a touch and DAS decides, as it did before.
--
-- A column rather than a constant because it is an on-chain address, which
-- lives only in config/collections.toml, and the ingester sees the registry
-- through Postgres alone (config/ ships in the admin image only).
ALTER TABLE collections
    ADD COLUMN program text COLLATE "C"
        CHECK (program IS NULL OR is_pubkey(program)),
    -- (IS NOT DISTINCT FROM: a NULL standard must not make the CHECK pass.)
    ADD CONSTRAINT collections_program_core_only
        CHECK (program IS NULL OR standard IS NOT DISTINCT FROM 'core');
