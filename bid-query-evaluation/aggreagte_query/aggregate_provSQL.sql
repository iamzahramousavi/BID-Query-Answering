-- ============================================================
-- Aggregate Query Evaluation
--
-- ProvSQL version: 1.12.0
--
-- Input relations are uploaded by:
--
--     aggregate_query.py
--
-- Relations:
--
-- R1p(
--     block_id,
--     eid,
--     education,
--     experience,
--     position,
--     p1
-- )
--
-- R2p(
--     block_id,
--     eid,
--     position,
--     salaryband,
--     defaultsalary,
--     p2
-- )
--
-- The aggregate query joins the relations using:
--
--     R1.eid = R2.eid
--
-- Query Q2 computes AVG(DefaultSalary) for tuples satisfying:
--
--     Education  = PhD
--     Experience = 15-30+
--     Position   = Manager
--     SalaryBand = 121k+
-- ============================================================


-- ============================================================
-- 1. ENVIRONMENT
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


SET search_path TO
    provsql_test,
    public,
    provsql;


-- ============================================================
-- 2. VERIFY UPLOADED DATA
-- ============================================================

SELECT
    COUNT(*)
        AS r1_rows,

    COUNT(DISTINCT block_id)
        AS r1_blocks,

    COUNT(DISTINCT eid)
        AS r1_eids

FROM
    provsql_test.R1p;


SELECT
    COUNT(*)
        AS r2_rows,

    COUNT(DISTINCT block_id)
        AS r2_blocks,

    COUNT(DISTINCT eid)
        AS r2_eids

FROM
    provsql_test.R2p;


-- ============================================================
-- 3. VERIFY EXPERIENCE CATEGORIES
--
-- The datasets used in this experiment contain the category:
--
--     15-30+
--
-- rather than:
--
--     15-25
-- ============================================================

SELECT DISTINCT
    experience

FROM
    provsql_test.R1p

ORDER BY
    experience;


-- ============================================================
-- 4. VERIFY eid JOIN
-- ============================================================

SELECT
    COUNT(*)
        AS eid_join_rows

FROM
    provsql_test.R1p r1

JOIN
    provsql_test.R2p r2
      ON r1.eid = r2.eid;


-- ============================================================
-- 5. SHOW Q2 MATCHING TUPLES BEFORE repair_key
--
-- This is a diagnostic query.
-- ============================================================

SELECT
    r1.block_id
        AS r1_block_id,

    r1.eid,

    r1.education,

    r1.experience,

    r1.position
        AS r1_position,

    r2.block_id
        AS r2_block_id,

    r2.position
        AS r2_position,

    r2.salaryband,

    r2.defaultsalary,

    r1.p1,

    r2.p2

FROM
    provsql_test.R1p r1

JOIN
    provsql_test.R2p r2
      ON r1.eid = r2.eid

WHERE
    r1.education = 'PhD'

    AND
    r1.experience = '15-30+'

    AND
    r1.position = 'Manager'

    AND
    r2.salaryband = '121k+'

ORDER BY
    r1.eid;


-- ============================================================
-- 6. APPLY repair_key
--
-- The repair_key operator represents the BID block semantics.
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
-- 7. ASSIGN ORIGINAL BID PROBABILITIES
-- ============================================================

DO $$
BEGIN

    PERFORM set_prob(
        provenance(),
        p1
    )
    FROM
        provsql_test.R1p;


    PERFORM set_prob(
        provenance(),
        p2
    )
    FROM
        provsql_test.R2p;

END;
$$;


-- ============================================================
-- 8. Q2
--
-- AVG(DefaultSalary)
--
-- Conditions:
--
--     Education  = PhD
--     Experience = 15-30+
--     Position   = Manager
--     SalaryBand = 121k+
--
-- Join:
--
--     R1.eid = R2.eid
-- ============================================================

SELECT
    AVG(
        r2.defaultsalary
    )::numeric
        AS q2_avg_default_salary

FROM
    provsql_test.R1p r1

JOIN
    provsql_test.R2p r2
      ON r1.eid = r2.eid

WHERE
    r1.education = 'PhD'

    AND
    r1.experience = '15-30+'

    AND
    r1.position = 'Manager'

    AND
    r2.salaryband = '121k+';


-- ============================================================
-- 9. Q2 MATCHING-ROW DIAGNOSTIC
-- ============================================================

SELECT
    COUNT(*)::bigint
        AS q2_matching_rows

FROM
    provsql_test.R1p r1

JOIN
    provsql_test.R2p r2
      ON r1.eid = r2.eid

WHERE
    r1.education = 'PhD'

    AND
    r1.experience = '15-30+'

    AND
    r1.position = 'Manager'

    AND
    r2.salaryband = '121k+';


-- ============================================================
-- 10. SHOW Q2 MATCHING TUPLES AFTER repair_key
-- ============================================================

SELECT
    r1.block_id
        AS r1_block_id,

    r1.eid,

    r1.education,

    r1.experience,

    r1.position
        AS r1_position,

    r2.block_id
        AS r2_block_id,

    r2.position
        AS r2_position,

    r2.salaryband,

    r2.defaultsalary

FROM
    provsql_test.R1p r1

JOIN
    provsql_test.R2p r2
      ON r1.eid = r2.eid

WHERE
    r1.education = 'PhD'

    AND
    r1.experience = '15-30+'

    AND
    r1.position = 'Manager'

    AND
    r2.salaryband = '121k+'

ORDER BY
    r1.eid;
