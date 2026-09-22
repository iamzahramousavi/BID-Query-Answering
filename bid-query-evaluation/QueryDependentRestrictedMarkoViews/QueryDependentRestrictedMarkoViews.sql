-- ============================================================
-- Query-Dependent Restricted MarkoViews
--
-- ProvSQL version: 1.12.0
--
-- This SQL file defines the preprocessing functions and the
-- ProvSQL probability-evaluation function required by:
--
--     QueryDependentRestrictedMarkoViews.py
--
-- The Python program creates the following input relations:
--
--     r1_prime_input
--     r1_vboth_input
--     r1_vnone_input
--
--     r2_prime_input
--     r2_vboth_input
--     r2_vnone_input
--
-- The current Boolean conjunctive query joins R1 and R2 on:
--
--     r1.eid = r2.eid
--
-- with the conditions:
--
--     r1.education  = 'PhD'
--     r1.experience = '0-15'
--     r1.position   = 'Senior'
--     r2.salaryband = '121k+'
--
-- Position is a selection attribute and is NOT the join
-- attribute.
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
-- These tables are recreated and populated by the Python
-- program before preprocessing.
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
-- 5. PREPROCESS R1
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
-- The resulting provenance-aware relations are:
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
    -- Remove previous preprocessed R1 relations
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
    -- Each row identifies a pair of tuples from the same BID
    -- block. Selecting both tuples constitutes an at-most-one
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
    -- One row is associated with each BID block and is used
    -- to construct the at-least-one violation event.
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
-- 6. PREPROCESS R2
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
-- The resulting provenance-aware relations are:
--
--     r2_prime
--     r2_vboth
--     r2_vnone
--
-- DefaultSalary is not required for the Boolean query used in
-- the Query-Dependent Restricted MarkoViews experiment.
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
    -- Remove previous preprocessed R2 relations
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
-- 7. PROVSQL EVENT-PROBABILITY FUNCTION
--
-- p1_E(event_sql) evaluates the probability of a Boolean
-- event in the auxiliary Tuple-Independent probabilistic
-- Database used by the MarkoViews translation.
--
-- The default compiler below is:
--
--     dsharp
--
-- To reproduce the experiment with d4, change:
--
--     'dsharp'
--
-- to:
--
--     'd4'
--
-- in the probability_evaluate call below, rerun this function
-- definition, and then rerun the Python program.
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
        'dsharp',
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
-- 8. VERIFY DATABASE AND PROVSQL VERSION
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
-- 9. VERIFY REQUIRED FUNCTIONS
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
