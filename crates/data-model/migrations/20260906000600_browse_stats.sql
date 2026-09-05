-- Browse-facing stats (ALG-625). The API contract's `CollectionStats` asks for
-- four numbers `collection_stats` did not carry: `burned`, `indexed` (the
-- browse population — members INCLUDING burned, i.e. the unfiltered result
-- count), `last_activity_at`, and the cohorts behind `holderDistribution`.
--
-- Appending columns to the view rather than adding a table: these are the same
-- correlated counts the view already runs, they are read once per collection
-- behind a short-TTL cache, and a second home for "supply" would eventually
-- disagree with the first.
CREATE OR REPLACE VIEW collection_stats AS
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
           AND x.block_time >= now() - interval '7 days')::integer           AS activity_7d,
       (SELECT count(*)              FROM assets a
         WHERE a.collection_id = c.id AND a.membership_status = 'member'
           AND a.burned)::integer                                           AS burned,
       -- THE browse population, and the contract's unfiltered result count.
       -- Deliberately `supply + burned`, from the same predicate every list
       -- and facet query applies.
       (SELECT count(*)              FROM assets a
         WHERE a.collection_id = c.id AND a.membership_status = 'member')::integer
                                                                            AS indexed,
       (SELECT max(a.last_activity_at) FROM assets a
         WHERE a.collection_id = c.id AND a.membership_status = 'member')   AS last_activity_at
FROM collections c;
