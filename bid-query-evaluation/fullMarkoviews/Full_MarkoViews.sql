-- ============================================================
-- Full / Global MarkoViews
--
-- ProvSQL version: 1.12.0
--
-- This SQL file defines the preprocessing functions and
-- probability-evaluation function required by:
--
--     Full_MarkoViews.py
--
-- The Python program constructs the auxiliary
-- Tuple-Independent probabilistic Database used by the
-- MarkoViews translation and creates:
--
--     r1_prime_input
--     r1_vboth_input
--     r1_vnone_input
--
--     r2_prime_input
--     r2_vboth_input
--     r2_vnone_input
--
-- The Boolean query evaluated in the experiment is:
--
--     SELECT TRUE AS k
--     FROM R1 r1
--     JOIN R2 r2
--       ON r1.eid = r2.eid
--     WHERE r1.education = 'PhD'
--       AND r1.experience = '0-15'
--       AND r1.position = 'Senior'
--       AND r2.salaryband = '121k+';
--
-- Position is a selection attribute and is NOT the join
-- attribute.
--
-- Full MarkoViews evaluates:
--
--            P1(Q AND NOT U)
--     P0(Q) = ----------------
--                P1(NOT U)
--
-- directly, avoiding the numerical cancellation that can
-- occur when computing 1 - P1(U).
-- ============================================================


-- ============================================================
-- 1. SCHEMA AND SEARCH PATH
-- ============================================================

CREATE SCHEMA IF NOT EXISTS provsql_test;

SET search_path TO
    provsql_test,
    public,
    provsql;


-- ============================================================
-- 2. CLEAN PREVIOUS FUNCTIONS
-- ============================================================

DROP FUNCTION IF EXISTS
    provsql_test.preprocess_r1()
CASCADE;

DROP FUNCTION IF EXISTS
    provsql_test.preprocess_r2()
CASCADE;

DROP FUNCTION IF EXISTS
    provsql_test.p1_E(text)
CASCADE;


-- ============================================================
-- 3. CLEAN PREVIOUS INPUT TABLES
--
-- These tables are recreated and populated by
-- Full_MarkoViews.py.
-- ============================================================

DROP TABLE IF EXISTS
    provsql_test.r1_prime_input
CASCADE;

DROP TABLE IF EXISTS
    provsql_test.r1_vboth_input
CASCADE;

DROP TABLE IF EXISTS
    provsql_test.r1_vnone_input
CASCADE;


DROP TABLE IF EXISTS
    provsql_test.r2_prime_input
CASCADE;

DROP TABLE IF EXISTS
    provsql_test.r2_vboth_input
CASCADE;

DROP TABLE IF EXISTS
    provsql_test.r2_vnone_input
CASCADE;


-- ============================================================
-- 4. CLEAN PREVIOUS PREPROCESSED TABLES
-- ============================================================

DROP TABLE IF EXISTS
    provsql_test.r1_prime
CASCADE;

DROP TABLE IF EXISTS
    provsql_test.r1_vboth
CASCADE;

DROP TABLE IF EXISTS
    provsql_test.r1_vnone
CASCADE;


DROP TABLE IF EXISTS
    provsql_test.r2_prime
CASCADE;

DROP TABLE IF EXISTS
    provsql_test.r2_vboth
CASCADE;

DROP TABLE IF EXISTS
    provsql_test.r2_vnone
CASCADE;


-- ============================================================
-- 5. CERTAIN TRUE EVENT
--
-- A provenance-aware event with probability 1 is needed to
-- construct:
--
--     NOT U = TRUE EXCEPT U
--
-- directly at the provenance level.
--
-- This avoids calculating:
--
--     1 - P1(U)
--
-- numerically when P1(U) is extremely close to 1.
-- ============================================================

DROP TABLE IF EXISTS
    provsql_test.truth_event
CASCADE;


CREATE TABLE
provsql_test.truth_event
(
    k BOOLEAN
);


INSERT INTO
    provsql_test.truth_event(k)
VALUES
    (TRUE);


SELECT provsql.add_provenance(
    'provsql_test.truth_event'::regclass
);


DO $$
BEGIN

    PERFORM provsql.set_prob(
        provenance(),
        1.0
    )

    FROM
        provsql_test.truth_event;

END;
$$;


-- ============================================================
-- 6. PREPROCESS R1
--
-- Input schema:
--
--     tuple_id
--     block_id
--     eid
--     education
--     experience
--     position
--     p0
--
-- Resulting provenance-aware relations:
--
--     r1_prime
--     r1_vboth
--     r1_vnone
-- ============================================================

CREATE OR REPLACE FUNCTION
provsql_test.preprocess_r1()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN

    PERFORM set_config(
        'search_path',
        'provsql_test,public,provsql',
        true
    );


    -- --------------------------------------------------------
    -- Remove previous R1 preprocessing
    -- --------------------------------------------------------

    DROP TABLE IF EXISTS
        provsql_test.r1_prime
    CASCADE;

    DROP TABLE IF EXISTS
        provsql_test.r1_vboth
    CASCADE;

    DROP TABLE IF EXISTS
        provsql_test.r1_vnone
    CASCADE;


    -- --------------------------------------------------------
    -- R1 PRIME
    --
    -- Each tuple has auxiliary TID probability:
    --
    --     p0 = p / (1 + p)
    --
    -- calculated by the Python program.
    -- --------------------------------------------------------

    CREATE TABLE
    provsql_test.r1_prime
    AS

    SELECT
        tuple_id,
        block_id,
        eid,
        education,
        experience,
        position,
        p0

    FROM
        provsql_test.r1_prime_input;


    PERFORM provsql.add_provenance(
        'provsql_test.r1_prime'::regclass
    );


    PERFORM provsql.set_prob(
        provenance(),
        p0
    )

    FROM
        provsql_test.r1_prime;


    -- --------------------------------------------------------
    -- R1 VBOTH
    --
    -- Each row identifies two tuples belonging to the same
    -- BID block. Selecting both constitutes an at-most-one
    -- violation.
    -- --------------------------------------------------------

    CREATE TABLE
    provsql_test.r1_vboth
    AS

    SELECT
        block_id,
        tuple_id_1,
        tuple_id_2,
        p0

    FROM
        provsql_test.r1_vboth_input;


    PERFORM provsql.add_provenance(
        'provsql_test.r1_vboth'::regclass
    );


    PERFORM provsql.set_prob(
        provenance(),
        p0
    )

    FROM
        provsql_test.r1_vboth;


    -- --------------------------------------------------------
    -- R1 VNONE
    --
    -- One helper row per BID block is used to construct the
    -- at-least-one violation event.
    -- --------------------------------------------------------

    CREATE TABLE
    provsql_test.r1_vnone
    AS

    SELECT
        block_id,
        p0

    FROM
        provsql_test.r1_vnone_input;


    PERFORM provsql.add_provenance(
        'provsql_test.r1_vnone'::regclass
    );


    PERFORM provsql.set_prob(
        provenance(),
        p0
    )

    FROM
        provsql_test.r1_vnone;


    -- --------------------------------------------------------
    -- R1 INDEXES
    -- --------------------------------------------------------

    CREATE INDEX IF NOT EXISTS
        idx_r1_prime_tuple_id
    ON
        provsql_test.r1_prime(tuple_id);


    CREATE INDEX IF NOT EXISTS
        idx_r1_prime_block_id
    ON
        provsql_test.r1_prime(block_id);


    CREATE INDEX IF NOT EXISTS
        idx_r1_prime_eid
    ON
        provsql_test.r1_prime(eid);


    CREATE INDEX IF NOT EXISTS
        idx_r1_prime_position
    ON
        provsql_test.r1_prime(position);


    CREATE INDEX IF NOT EXISTS
        idx_r1_prime_query
    ON
        provsql_test.r1_prime(
            block_id,
            eid,
            education,
            experience,
            position
        );


    CREATE INDEX IF NOT EXISTS
        idx_r1_vboth_block_pair
    ON
        provsql_test.r1_vboth(
            block_id,
            tuple_id_1,
            tuple_id_2
        );


    CREATE INDEX IF NOT EXISTS
        idx_r1_vnone_block_id
    ON
        provsql_test.r1_vnone(block_id);


    ANALYZE
        provsql_test.r1_prime;

    ANALYZE
        provsql_test.r1_vboth;

    ANALYZE
        provsql_test.r1_vnone;

END;
$$;


-- ============================================================
-- 7. PREPROCESS R2
--
-- Input schema:
--
--     tuple_id
--     block_id
--     eid
--     position
--     salaryband
--     p0
--
-- Resulting provenance-aware relations:
--
--     r2_prime
--     r2_vboth
--     r2_vnone
-- ============================================================

CREATE OR REPLACE FUNCTION
provsql_test.preprocess_r2()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN

    PERFORM set_config(
        'search_path',
        'provsql_test,public,provsql',
        true
    );


    -- --------------------------------------------------------
    -- Remove previous R2 preprocessing
    -- --------------------------------------------------------

    DROP TABLE IF EXISTS
        provsql_test.r2_prime
    CASCADE;

    DROP TABLE IF EXISTS
        provsql_test.r2_vboth
    CASCADE;

    DROP TABLE IF EXISTS
        provsql_test.r2_vnone
    CASCADE;


    -- --------------------------------------------------------
    -- R2 PRIME
    -- --------------------------------------------------------

    CREATE TABLE
    provsql_test.r2_prime
    AS

    SELECT
        tuple_id,
        block_id,
        eid,
        position,
        salaryband,
        p0

    FROM
        provsql_test.r2_prime_input;


    PERFORM provsql.add_provenance(
        'provsql_test.r2_prime'::regclass
    );


    PERFORM provsql.set_prob(
        provenance(),
        p0
    )

    FROM
        provsql_test.r2_prime;


    -- --------------------------------------------------------
    -- R2 VBOTH
    -- --------------------------------------------------------

    CREATE TABLE
    provsql_test.r2_vboth
    AS

    SELECT
        block_id,
        tuple_id_1,
        tuple_id_2,
        p0

    FROM
        provsql_test.r2_vboth_input;


    PERFORM provsql.add_provenance(
        'provsql_test.r2_vboth'::regclass
    );


    PERFORM provsql.set_prob(
        provenance(),
        p0
    )

    FROM
        provsql_test.r2_vboth;


    -- --------------------------------------------------------
    -- R2 VNONE
    -- --------------------------------------------------------

    CREATE TABLE
    provsql_test.r2_vnone
    AS

    SELECT
        block_id,
        p0

    FROM
        provsql_test.r2_vnone_input;


    PERFORM provsql.add_provenance(
        'provsql_test.r2_vnone'::regclass
    );


    PERFORM provsql.set_prob(
        provenance(),
        p0
    )

    FROM
        provsql_test.r2_vnone;


    -- --------------------------------------------------------
    -- R2 INDEXES
    -- --------------------------------------------------------

    CREATE INDEX IF NOT EXISTS
        idx_r2_prime_tuple_id
    ON
        provsql_test.r2_prime(tuple_id);


    CREATE INDEX IF NOT EXISTS
        idx_r2_prime_block_id
    ON
        provsql_test.r2_prime(block_id);


    CREATE INDEX IF NOT EXISTS
        idx_r2_prime_eid
    ON
        provsql_test.r2_prime(eid);


    CREATE INDEX IF NOT EXISTS
        idx_r2_prime_position
    ON
        provsql_test.r2_prime(position);


    CREATE INDEX IF NOT EXISTS
        idx_r2_prime_query
    ON
        provsql_test.r2_prime(
            block_id,
            eid,
            position,
            salaryband
        );


    CREATE INDEX IF NOT EXISTS
        idx_r2_vboth_block_pair
    ON
        provsql_test.r2_vboth(
            block_id,
            tuple_id_1,
            tuple_id_2
        );


    CREATE INDEX IF NOT EXISTS
        idx_r2_vnone_block_id
    ON
        provsql_test.r2_vnone(block_id);


    ANALYZE
        provsql_test.r2_prime;

    ANALYZE
        provsql_test.r2_vboth;

    ANALYZE
        provsql_test.r2_vnone;

END;
$$;


-- ============================================================
-- 8. PROVSQL EVENT-PROBABILITY FUNCTION
--
-- p1_E(event_sql) evaluates an event probability in the
-- auxiliary Tuple-Independent probabilistic Database.
--
-- The Full MarkoViews Python implementation uses it to
-- evaluate directly:
--
--     P1(NOT U)
--
-- and:
--
--     P1(Q AND NOT U)
--
-- rather than recovering these quantities through subtraction.
--
-- ============================================================
-- KNOWLEDGE COMPILER
--
-- Default below:
--
--     d4
--
-- To reproduce the dsharp experiment, change:
--
--     'd4'
--
-- to:
--
--     'dsharp'
--
-- in probability_evaluate below, rerun this SQL file, and run
-- the same Python program again.
-- ============================================================

CREATE OR REPLACE FUNCTION
provsql_test.p1_E(
    event_sql text
)
RETURNS double precision
LANGUAGE plpgsql
AS $$
DECLARE

    result_val double precision;

BEGIN

    EXECUTE format(
        'SELECT COALESCE(
             provsql.probability_evaluate(
                 provenance(),
                 %L,
                 %L
             ),
             0.0
         )
         FROM (%s) ev
         GROUP BY k',

        'compilation',
        'd4',
        event_sql
    )

    INTO result_val;


    RETURN COALESCE(
        result_val,
        0.0
    );

END;
$$;


-- ============================================================
-- 9. VERIFY DATABASE ENVIRONMENT
-- ============================================================

SELECT
    current_database()
        AS database_name,

    current_user
        AS database_user,

    (
        SELECT
            extversion

        FROM
            pg_extension

        WHERE
            extname = 'provsql'
    )
        AS provsql_version;


-- ============================================================
-- 10. VERIFY REQUIRED FUNCTIONS
-- ============================================================

SELECT
    proname

FROM
    pg_proc p

JOIN
    pg_namespace n
      ON n.oid = p.pronamespace

WHERE
    n.nspname = 'provsql_test'

    AND p.proname IN (
        'preprocess_r1',
        'preprocess_r2',
        'p1_e'
    )

ORDER BY
    proname;


-- ============================================================
-- 11. VERIFY CERTAIN TRUE EVENT
-- ============================================================

SELECT
    k,

    probability_evaluate(
        provenance(),
        'compilation',
        'd4'
    )
        AS probability

FROM
    provsql_test.truth_event

GROUP BY
    k;
