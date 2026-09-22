-- ============================================================
-- Repair-key evaluation of a Boolean conjunctive query
--
-- ProvSQL version: 1.12.0
--
-- Input relations are uploaded by Repair_key.py:
--
--   provsql_test.r1_bid_input
--   provsql_test.r2_bid_input
--
-- Query:
--
--   SELECT TRUE AS k
--   FROM R1 r1
--   JOIN R2 r2
--     ON r1.eid = r2.eid
--   WHERE r1.education = 'PhD'
--     AND r1.experience = '0-15'
--     AND r1.position = 'Senior'
--     AND r2.salaryband = '121k+';
--
-- The same repair-key provenance is evaluated using:
--
--   1. ProvSQL possible-worlds
--   2. d4
--   3. dsharp
-- ============================================================


-- ============================================================
-- 1. ENVIRONMENT CHECK
-- ============================================================

SELECT
    current_database() AS database_name,
    current_user AS database_user,
    inet_server_addr() AS server_address,
    inet_server_port() AS internal_server_port,
    (
        SELECT extversion
        FROM pg_extension
        WHERE extname = 'provsql'
    ) AS provsql_version;


SET search_path TO
    provsql_test,
    provsql,
    public;


-- ============================================================
-- 2. VERIFY INPUT RELATIONS
-- ============================================================

SELECT
    COUNT(*) AS r1_rows,
    COUNT(DISTINCT block_id) AS r1_blocks,
    COUNT(DISTINCT eid) AS r1_eids
FROM provsql_test.r1_bid_input;


SELECT
    COUNT(*) AS r2_rows,
    COUNT(DISTINCT block_id) AS r2_blocks,
    COUNT(DISTINCT eid) AS r2_eids
FROM provsql_test.r2_bid_input;


-- ============================================================
-- 3. CHECK THE eid JOIN
-- ============================================================

SELECT
    COUNT(*) AS eid_join_rows
FROM provsql_test.r1_bid_input r1
JOIN provsql_test.r2_bid_input r2
  ON r1.eid = r2.eid;


-- ============================================================
-- 4. SHOW QUERY WITNESSES IN THE INPUT BID RELATIONS
--
-- This is only a diagnostic query.
-- The number of witnesses depends on the dataset size.
-- ============================================================

SELECT
    r1.block_id AS r1_block,
    r1.eid,
    r1.education,
    r1.experience,
    r1.position AS r1_position,
    r1.p AS r1_probability,

    r2.block_id AS r2_block,
    r2.position AS r2_position,
    r2.salaryband,
    r2.defaultsalary,
    r2.p AS r2_probability

FROM provsql_test.r1_bid_input r1

JOIN provsql_test.r2_bid_input r2
  ON r1.eid = r2.eid

WHERE
    r1.education = 'PhD'
    AND r1.experience = '0-15'
    AND r1.position = 'Senior'
    AND r2.salaryband = '121k+'

ORDER BY
    r1.eid;


-- ============================================================
-- 5. CLEAN PREVIOUS REPAIR-KEY TABLES
-- ============================================================

DROP TABLE IF EXISTS
    provsql_test.R1p
CASCADE;

DROP TABLE IF EXISTS
    provsql_test.R2p
CASCADE;


-- ============================================================
-- 6. CREATE R1p
--
-- Duplicate projected tuples, if any, are combined by
-- summing their BID probabilities.
-- ============================================================

CREATE TABLE provsql_test.R1p AS

SELECT
    block_id,
    eid,
    education,
    experience,
    position,
    SUM(p)::double precision AS p1

FROM provsql_test.r1_bid_input

GROUP BY
    block_id,
    eid,
    education,
    experience,
    position;


-- ============================================================
-- 7. CREATE R2p
-- ============================================================

CREATE TABLE provsql_test.R2p AS

SELECT
    block_id,
    eid,
    position,
    salaryband,
    defaultsalary,
    SUM(p)::double precision AS p2

FROM provsql_test.r2_bid_input

GROUP BY
    block_id,
    eid,
    position,
    salaryband,
    defaultsalary;


-- ============================================================
-- 8. OPTIONAL INDEXES
-- ============================================================

CREATE INDEX idx_r1p_block_id
    ON provsql_test.R1p(block_id);

CREATE INDEX idx_r1p_eid
    ON provsql_test.R1p(eid);

CREATE INDEX idx_r2p_block_id
    ON provsql_test.R2p(block_id);

CREATE INDEX idx_r2p_eid
    ON provsql_test.R2p(eid);


ANALYZE provsql_test.R1p;
ANALYZE provsql_test.R2p;


-- ============================================================
-- 9. APPLY repair_key
--
-- repair_key enforces the block-level mutual-exclusion
-- structure used by the BID relations.
-- ============================================================

SELECT repair_key(
    'R1p',
    'block_id'
);


SELECT repair_key(
    'R2p',
    'block_id'
);


-- ============================================================
-- 10. ASSIGN ORIGINAL BID PROBABILITIES
-- ============================================================

DO $$
BEGIN

    PERFORM set_prob(
        provenance(),
        p1
    )
    FROM provsql_test.R1p;


    PERFORM set_prob(
        provenance(),
        p2
    )
    FROM provsql_test.R2p;

END $$;


-- ============================================================
-- 11. SHOW QUERY WITNESSES AFTER repair_key
-- ============================================================

SELECT
    r1.block_id AS r1_block,
    r1.eid,
    r1.education,
    r1.experience,
    r1.position AS r1_position,
    r1.p1,

    r2.block_id AS r2_block,
    r2.position AS r2_position,
    r2.salaryband,
    r2.defaultsalary,
    r2.p2

FROM provsql_test.R1p r1

JOIN provsql_test.R2p r2
  ON r1.eid = r2.eid

WHERE
    r1.education = 'PhD'
    AND r1.experience = '0-15'
    AND r1.position = 'Senior'
    AND r2.salaryband = '121k+'

ORDER BY
    r1.eid;


-- ============================================================
-- 12. OPTIONAL:
--     POSSIBLE-WORLDS PROBABILITY OF EACH INDIVIDUAL WITNESS
-- ============================================================

SELECT
    r1.eid,
    r1.p1,
    r2.p2,

    probability_evaluate(
        provenance(),
        'possible-worlds'
    ) AS witness_probability

FROM provsql_test.R1p r1

JOIN provsql_test.R2p r2
  ON r1.eid = r2.eid

WHERE
    r1.education = 'PhD'
    AND r1.experience = '0-15'
    AND r1.position = 'Senior'
    AND r2.salaryband = '121k+'

ORDER BY
    r1.eid;


-- ============================================================
-- 13. BOOLEAN QUERY:
--     PROVSQL POSSIBLE-WORLDS EVALUATION
--
-- For very large datasets this evaluator may be
-- computationally impractical.
-- ============================================================

SELECT
    k,

    probability_evaluate(
        provenance(),
        'possible-worlds'
    ) AS possible_worlds_probability

FROM (

    SELECT
        TRUE AS k

    FROM provsql_test.R1p r1

    JOIN provsql_test.R2p r2
      ON r1.eid = r2.eid

    WHERE
        r1.education = 'PhD'
        AND r1.experience = '0-15'
        AND r1.position = 'Senior'
        AND r2.salaryband = '121k+'

    GROUP BY
        k

) q;


-- ============================================================
-- 14. BOOLEAN QUERY:
--     KNOWLEDGE COMPILATION WITH d4
-- ============================================================

SELECT
    k,

    probability_evaluate(
        provenance(),
        'compilation',
        'd4'
    ) AS d4_probability

FROM (

    SELECT
        TRUE AS k

    FROM provsql_test.R1p r1

    JOIN provsql_test.R2p r2
      ON r1.eid = r2.eid

    WHERE
        r1.education = 'PhD'
        AND r1.experience = '0-15'
        AND r1.position = 'Senior'
        AND r2.salaryband = '121k+'

    GROUP BY
        k

) q;


-- ============================================================
-- 15. BOOLEAN QUERY:
--     KNOWLEDGE COMPILATION WITH dsharp
-- ============================================================

SELECT
    k,

    probability_evaluate(
        provenance(),
        'compilation',
        'dsharp'
    ) AS dsharp_probability

FROM (

    SELECT
        TRUE AS k

    FROM provsql_test.R1p r1

    JOIN provsql_test.R2p r2
      ON r1.eid = r2.eid

    WHERE
        r1.education = 'PhD'
        AND r1.experience = '0-15'
        AND r1.position = 'Senior'
        AND r2.salaryband = '121k+'

    GROUP BY
        k

) q;
