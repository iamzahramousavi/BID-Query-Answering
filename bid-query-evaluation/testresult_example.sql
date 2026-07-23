
-- TestResults BID Database
-- pipeline using MarkoViews and ProvSQL


SET search_path TO provsql_test, public, provsql;

CREATE SCHEMA IF NOT EXISTS provsql_test;


-- Drop existing tables and functions


DROP FUNCTION IF EXISTS provsql_test.preprocess_testresults() CASCADE;
DROP FUNCTION IF EXISTS provsql_test.p1_E(text)              CASCADE;

DROP TABLE IF EXISTS provsql_test.testresults_d0_input CASCADE;
DROP TABLE IF EXISTS provsql_test.testresults_prime    CASCADE;
DROP TABLE IF EXISTS provsql_test.v_diagboth           CASCADE;
DROP TABLE IF EXISTS provsql_test.v_diagnone           CASCADE;





CREATE TABLE provsql_test.testresults_d0_input (
    tuple_id  text,
    block_id  text,
    diagnosis text,
    p0        double precision
);

INSERT INTO provsql_test.testresults_d0_input VALUES
    ('TR__101__1', '101', 'Flu',   0.8),
    ('TR__101__2', '101', 'Cold',  0.2),
    ('TR__102__1', '102', 'Strep', 0.6),
    ('TR__102__2', '102', 'Mono',  0.4);



-- Derives all D1 tables automatically from D0:
--   testresults_prime : p1' = p0 / (1 + p0)
--   v_diagboth        : self-join on block_id (at-most-one)
--   v_diagnone        : one row per block (at-least-one)
-- Then makes all three tables ProvSQL-aware.


CREATE OR REPLACE FUNCTION provsql_test.preprocess_testresults()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM set_config('search_path', 'provsql_test,public,provsql', true);

  DROP TABLE IF EXISTS provsql_test.testresults_prime CASCADE;
  DROP TABLE IF EXISTS provsql_test.v_diagboth        CASCADE;
  DROP TABLE IF EXISTS provsql_test.v_diagnone        CASCADE;

  -- testresults_prime: TID translation, p1' = p0 / (1 + p0)
  CREATE TABLE provsql_test.testresults_prime AS
  SELECT
      tuple_id,
      block_id,
      diagnosis,
      (p0 / (1.0 + p0))::double precision AS p0
  FROM provsql_test.testresults_d0_input;

  PERFORM provsql.add_provenance('provsql_test.testresults_prime'::regclass);
  PERFORM provsql.set_prob(provenance(), p0)
  FROM provsql_test.testresults_prime;

  -- v_diagboth: at-most-one violation table
  -- One row per pair of tuples in the same block (p_V = 1)
  -- Derived by self-join on block_id with tuple_id_1 < tuple_id_2
  CREATE TABLE provsql_test.v_diagboth AS
  SELECT
      t1.block_id,
      t1.tuple_id AS tuple_id_1,
      t2.tuple_id AS tuple_id_2,
      1.0::double precision AS p0
  FROM provsql_test.testresults_d0_input t1
  JOIN provsql_test.testresults_d0_input t2
    ON t1.block_id = t2.block_id
   AND t1.tuple_id < t2.tuple_id;

  PERFORM provsql.add_provenance('provsql_test.v_diagboth'::regclass);
  PERFORM provsql.set_prob(provenance(), p0)
  FROM provsql_test.v_diagboth;

  -- v_diagnone: at-least-one violation table
  -- One row per distinct block (p_V = 1)
  -- Derived by GROUP BY block_id
  CREATE TABLE provsql_test.v_diagnone AS
  SELECT
      block_id,
      1.0::double precision AS p0
  FROM provsql_test.testresults_d0_input
  GROUP BY block_id;

  PERFORM provsql.add_provenance('provsql_test.v_diagnone'::regclass);
  PERFORM provsql.set_prob(provenance(), p0)
  FROM provsql_test.v_diagnone;

  ANALYZE provsql_test.testresults_prime;
  ANALYZE provsql_test.v_diagboth;
  ANALYZE provsql_test.v_diagnone;
END;
$$;




-- Evaluates P1(U) via ProvSQL probability_evaluate


CREATE OR REPLACE FUNCTION provsql_test.p1_E(event_sql text)
RETURNS double precision
LANGUAGE plpgsql
AS $$
DECLARE
  result_val double precision;
BEGIN
  EXECUTE format(
    'SELECT COALESCE(
         provsql.probability_evaluate(provenance(), %L, %L),
         0.0
     )
     FROM (%s) ev
     GROUP BY k',
    'compilation',
    'd4',
    event_sql
  )
  INTO result_val;

  RETURN COALESCE(result_val, 0.0);
END;
$$;




SELECT provsql_test.preprocess_testresults();




-- testresults_prime: base TID table with translated p1'
SELECT
    tuple_id,
    block_id,
    diagnosis,
    round(p0::numeric, 6) AS p1_prime
FROM provsql_test.testresults_prime
ORDER BY block_id, tuple_id;

-- v_diagboth: at-most-one violation table (p_V = 1)
SELECT
    block_id,
    tuple_id_1,
    tuple_id_2,
    p0 AS p_v
FROM provsql_test.v_diagboth
ORDER BY block_id;

-- v_diagnone: at-least-one violation table (p_V = 1)
SELECT
    block_id,
    p0 AS p_v
FROM provsql_test.v_diagnone
ORDER BY block_id;




SELECT
    round(p1_u::numeric,      12) AS "P1(U)",
    round(p1_q_or_u::numeric, 12) AS "P1(Q or U)",
    round(
        ((p1_q_or_u - p1_u) / (1.0 - p1_u))::numeric
    , 12)                          AS "P0(Q)"
FROM (
    SELECT
        provsql_test.p1_E(
            'SELECT TRUE AS k
             FROM provsql_test.v_diagboth vb
             JOIN provsql_test.testresults_prime t1
               ON vb.tuple_id_1 = t1.tuple_id
             JOIN provsql_test.testresults_prime t2
               ON vb.tuple_id_2 = t2.tuple_id
             GROUP BY k
             UNION ALL
             SELECT TRUE AS k
             FROM (
               SELECT vn.block_id
               FROM provsql_test.v_diagnone vn
               EXCEPT
               SELECT t.block_id
               FROM provsql_test.testresults_prime t
               GROUP BY t.block_id
             ) un'
        ) AS p1_u,

        provsql_test.p1_E(
            'SELECT TRUE AS k
             FROM provsql_test.testresults_prime
             WHERE block_id  = ''101''
               AND diagnosis = ''Flu''
             GROUP BY k
             UNION ALL
             SELECT TRUE AS k
             FROM provsql_test.v_diagboth vb
             JOIN provsql_test.testresults_prime t1
               ON vb.tuple_id_1 = t1.tuple_id
             JOIN provsql_test.testresults_prime t2
               ON vb.tuple_id_2 = t2.tuple_id
             GROUP BY k
             UNION ALL
             SELECT TRUE AS k
             FROM (
               SELECT vn.block_id
               FROM provsql_test.v_diagnone vn
               EXCEPT
               SELECT t.block_id
               FROM provsql_test.testresults_prime t
               GROUP BY t.block_id
             ) un'
        ) AS p1_q_or_u

) vals;




-- Repair-Key method for the same TestResults example


DROP TABLE IF EXISTS provsql_test.rk_testresults CASCADE;
DROP TABLE IF EXISTS provsql_test.rk_result       CASCADE;




CREATE TABLE provsql_test.rk_testresults AS
SELECT
    block_id,
    diagnosis,
    p0 AS p
FROM provsql_test.testresults_d0_input;





SELECT repair_key('rk_testresults', 'block_id');



DO $$
BEGIN
    PERFORM set_prob(provenance(), p)
    FROM provsql_test.rk_testresults;
END $$;



SELECT block_id, diagnosis, p
FROM provsql_test.rk_testresults
ORDER BY block_id, diagnosis;




CREATE TABLE provsql_test.rk_result AS
SELECT
    *,
    probability_evaluate(provenance(), 'compilation', 'd4') AS prob
FROM (
    SELECT TRUE AS k
    FROM provsql_test.rk_testresults
    WHERE block_id  = '101'
      AND diagnosis = 'Flu'
    GROUP BY k
) q;




SELECT
    prob                  AS "P0(Q) repair-key"
FROM provsql_test.rk_result;




