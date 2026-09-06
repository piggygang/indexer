-- Statistical rarity (ALG-627): a score and a 1..N rank per asset, plus the
-- bookkeeping that keeps them fresh.
--
-- THE FORMULA, stated once, here, because every other statement about it is a
-- restatement of this one:
--
--   population P = collection_id = $1 AND membership_status = 'member'
--                  -- the browse population VERBATIM: burned assets are in
--                  -- (the UI greys them), removed assets are out
--   N           = |P|
--   trait set T = the collection's trait_types WHERE is_facet
--   for t in T, the asset's value v, or the sentinel "absent" when it has none
--   c(t, v)     = how many members of P have that cell
--
--   score = SUM over t in T of N / c(t, v)      -- summed in numeric, 6 dp
--   rank  = row_number() OVER (ORDER BY score DESC, id)
--
-- Three choices the numbers forced, recorded so they are not re-litigated:
--
--   * ABSENCE IS A VALUE, not a skipped term. Every asset's score is then a sum
--     over the same |T| terms, so scores are comparable. Skipping would reward
--     an asset merely for having a slot filled: on the Core collection an
--     Earring is worn by 258 of 747, so "wears one at all" IS the rarity
--     signal, and the 489 that share the absent bucket rightly contribute
--     almost nothing. Measured: the two treatments disagree on 723 of 747
--     ranks, by up to 280 places.
--   * FACETABLE TRAIT TYPES ONLY. `is_facet` is exactly the set /facets counts,
--     so the sidebar and the score can never disagree about which traits are
--     real, and a per-asset-unique "Name" stays out. A collection with no
--     facetable trait types (Pig Mud, whose metadata host is gone) scores NULL
--     rather than producing an N-way tie.
--   * SUMMED IN numeric, stored as double precision. float8 addition is not
--     associative, and the same query produced three different results under
--     three planner settings (parallel aggregate on/off, hashagg off) — which
--     would make `rarity --expect-unchanged` a coin flip and rewrite every row
--     on every pass. numeric is exact and order-independent by construction.
ALTER TABLE assets
    ADD COLUMN rarity_score double precision,
    ADD COLUMN rarity_rank  integer;

-- Written together by one statement, so one without the other is a bug, not a
-- state. `minimum: 1` is the API contract's own constraint on rarityRank.
ALTER TABLE assets
    ADD CONSTRAINT assets_rarity_pair
        CHECK ((rarity_score IS NULL) = (rarity_rank IS NULL)),
    ADD CONSTRAINT assets_rarity_rank_positive
        CHECK (rarity_rank IS NULL OR rarity_rank >= 1);

-- sort=rarity, keyset-paginated. The NULL sentinel lives INSIDE the index
-- expression for the same reason it does on assets_browse_number: rank 1 is
-- the rarest and the sort ascends, so an unranked asset must sort last and a
-- keyset cursor must still be able to walk over it. One index serves both
-- directions through a backward scan.
CREATE INDEX assets_browse_rarity
    ON assets (collection_id, (coalesce(rarity_rank, 2147483647)), id);

-- Recompute bookkeeping.
--
-- rarity_dirty defaults to TRUE on purpose: the scheduler seeds a job with no
-- backfill_state row as "finished now" (so a fresh deploy does not kick off a
-- full deep pass a minute after boot), which would otherwise leave a brand-new
-- database serving rarityRank: null for a whole interval. A column default
-- closes that with no code.
--
-- rarity_version is bumped only by a pass that actually changed rows. It fences
-- rarity cursors: one mint re-ranks 506 of 746 assets, so a cursor issued
-- before a re-rank would silently skip and repeat rows. It is a fingerprint
-- input for rarity sorts ONLY, so no number/name/activity cursor is affected.
ALTER TABLE collections
    ADD COLUMN rarity_dirty   boolean NOT NULL DEFAULT true,
    ADD COLUMN rarity_version integer NOT NULL DEFAULT 0;

-- The integrity half: a collection whose ranks are not a clean 1..N over its
-- faceted population. Empty = healthy, like every other integrity_* view.
CREATE VIEW integrity_rarity_broken AS
SELECT c.id AS collection_id, c.slug,
       count(*) FILTER (WHERE a.rarity_rank IS NULL)::bigint AS unranked,
       count(*)::bigint                                      AS members,
       count(DISTINCT a.rarity_rank)::bigint                 AS distinct_ranks,
       max(a.rarity_rank)::bigint                            AS max_rank
  FROM collections c
  JOIN assets a ON a.collection_id = c.id AND a.membership_status = 'member'
 WHERE c.enabled
   AND EXISTS (SELECT 1 FROM trait_types tt WHERE tt.collection_id = c.id AND tt.is_facet)
 GROUP BY c.id, c.slug
HAVING count(*) FILTER (WHERE a.rarity_rank IS NULL) > 0
    OR count(DISTINCT a.rarity_rank) <> count(*)
    OR max(a.rarity_rank) <> count(*);
