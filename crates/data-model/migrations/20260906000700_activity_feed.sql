-- The collection activity feed (ALG-626). `/v1/collections/{slug}/activity` is
-- keyset-paginated on `(slot, id)` descending, and neither existing index can
-- serve that order: `activity_collection_time` orders by `block_time`, and
-- `activity_collection_kind_slot` puts `kind` ahead of `slot`, so an
-- unfiltered feed cannot use it for ordering. This is the third access path on
-- the same table, matching `activity_asset_timeline` one level up.
--
-- Built without CONCURRENTLY because sqlx wraps each migration in a
-- transaction: `activity` is small (tens of thousands of rows) and the build
-- is sub-second. A future index on a table large enough for that lock to
-- matter needs a different mechanism, not a bigger maintenance window.
CREATE INDEX activity_collection_slot ON activity (collection_id, slot DESC, id DESC);

-- `?kind=` on either feed rides on the same index as a filter-after-scan: a
-- per-asset timeline is at most a few hundred rows, and the collection feed's
-- kind filter is a narrowing of an already-ordered scan. A dedicated
-- (collection_id, kind, slot DESC, id DESC) index would only pay off once the
-- "latest mints" strip is hot enough to measure.
