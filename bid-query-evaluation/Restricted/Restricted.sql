
SET search_path TO provsql_test, public, provsql;

CREATE SCHEMA IF NOT EXISTS provsql_test;

DROP FUNCTION IF EXISTS provsql_test.preprocess_r1() CASCADE;
DROP FUNCTION IF EXISTS provsql_test.preprocess_r2() CASCADE;
DROP FUNCTION IF EXISTS provsql_test.p1_E(text) CASCADE;

DROP TABLE IF EXISTS provsql_test.r1_prime_input CASCADE;
DROP TABLE IF EXISTS provsql_test.r1_vboth_input CASCADE;
DROP TABLE IF EXISTS provsql_test.r1_vnone_input CASCADE;

DROP TABLE IF EXISTS provsql_test.r2_prime_input CASCADE;
DROP TABLE IF EXISTS provsql_test.r2_vboth_input CASCADE;
DROP TABLE IF EXISTS provsql_test.r2_vnone_input CASCADE;

DROP TABLE IF EXISTS provsql_test.r1_prime CASCADE;
DROP TABLE IF EXISTS provsql_test.r1_vboth CASCADE;
DROP TABLE IF EXISTS provsql_test.r1_vnone CASCADE;

DROP TABLE IF EXISTS provsql_test.r2_prime CASCADE;
DROP TABLE IF EXISTS provsql_test.r2_vboth CASCADE;
DROP TABLE IF EXISTS provsql_test.r2_vnone CASCADE;


CREATE OR REPLACE FUNCTION provsql_test.preprocess_r1()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM set_config('search_path', 'provsql_test,public,provsql', true);

  DROP TABLE IF EXISTS provsql_test.r1_prime CASCADE;
  DROP TABLE IF EXISTS provsql_test.r1_vboth CASCADE;
  DROP TABLE IF EXISTS provsql_test.r1_vnone CASCADE;

  CREATE TABLE provsql_test.r1_prime AS
  SELECT
      tuple_id,
      block_id,
      education,
      experience,
      position,
      p0
  FROM provsql_test.r1_prime_input;

  PERFORM provsql.add_provenance('provsql_test.r1_prime'::regclass);
  PERFORM provsql.set_prob(provenance(), p0)
  FROM provsql_test.r1_prime;

  CREATE TABLE provsql_test.r1_vboth AS
  SELECT
      block_id,
      tuple_id_1,
      tuple_id_2,
      p0
  FROM provsql_test.r1_vboth_input;

  PERFORM provsql.add_provenance('provsql_test.r1_vboth'::regclass);
  PERFORM provsql.set_prob(provenance(), p0)
  FROM provsql_test.r1_vboth;

  CREATE TABLE provsql_test.r1_vnone AS
  SELECT
      block_id,
      p0
  FROM provsql_test.r1_vnone_input;

  PERFORM provsql.add_provenance('provsql_test.r1_vnone'::regclass);
  PERFORM provsql.set_prob(provenance(), p0)
  FROM provsql_test.r1_vnone;

  CREATE INDEX IF NOT EXISTS idx_r1_prime_tuple_id
    ON provsql_test.r1_prime(tuple_id);

  CREATE INDEX IF NOT EXISTS idx_r1_prime_block_id
    ON provsql_test.r1_prime(block_id);

  CREATE INDEX IF NOT EXISTS idx_r1_prime_position
    ON provsql_test.r1_prime(position);

  CREATE INDEX IF NOT EXISTS idx_r1_prime_full
    ON provsql_test.r1_prime(block_id, education, experience, position);

  CREATE INDEX IF NOT EXISTS idx_r1_vboth_block_pair
    ON provsql_test.r1_vboth(block_id, tuple_id_1, tuple_id_2);

  CREATE INDEX IF NOT EXISTS idx_r1_vnone_block_id
    ON provsql_test.r1_vnone(block_id);

  ANALYZE provsql_test.r1_prime;
  ANALYZE provsql_test.r1_vboth;
  ANALYZE provsql_test.r1_vnone;
END;
$$;


CREATE OR REPLACE FUNCTION provsql_test.preprocess_r2()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM set_config('search_path', 'provsql_test,public,provsql', true);

  DROP TABLE IF EXISTS provsql_test.r2_prime CASCADE;
  DROP TABLE IF EXISTS provsql_test.r2_vboth CASCADE;
  DROP TABLE IF EXISTS provsql_test.r2_vnone CASCADE;

  CREATE TABLE provsql_test.r2_prime AS
  SELECT
      tuple_id,
      block_id,
      position,
      salaryband,
      p0
  FROM provsql_test.r2_prime_input;

  PERFORM provsql.add_provenance('provsql_test.r2_prime'::regclass);
  PERFORM provsql.set_prob(provenance(), p0)
  FROM provsql_test.r2_prime;

  CREATE TABLE provsql_test.r2_vboth AS
  SELECT
      block_id,
      tuple_id_1,
      tuple_id_2,
      p0
  FROM provsql_test.r2_vboth_input;

  PERFORM provsql.add_provenance('provsql_test.r2_vboth'::regclass);
  PERFORM provsql.set_prob(provenance(), p0)
  FROM provsql_test.r2_vboth;

  CREATE TABLE provsql_test.r2_vnone AS
  SELECT
      block_id,
      p0
  FROM provsql_test.r2_vnone_input;

  PERFORM provsql.add_provenance('provsql_test.r2_vnone'::regclass);
  PERFORM provsql.set_prob(provenance(), p0)
  FROM provsql_test.r2_vnone;

  CREATE INDEX IF NOT EXISTS idx_r2_prime_tuple_id
    ON provsql_test.r2_prime(tuple_id);

  CREATE INDEX IF NOT EXISTS idx_r2_prime_block_id
    ON provsql_test.r2_prime(block_id);

  CREATE INDEX IF NOT EXISTS idx_r2_prime_position
    ON provsql_test.r2_prime(position);

  CREATE INDEX IF NOT EXISTS idx_r2_prime_full
    ON provsql_test.r2_prime(block_id, position, salaryband);

  CREATE INDEX IF NOT EXISTS idx_r2_vboth_block_pair
    ON provsql_test.r2_vboth(block_id, tuple_id_1, tuple_id_2);

  CREATE INDEX IF NOT EXISTS idx_r2_vnone_block_id
    ON provsql_test.r2_vnone(block_id);

  ANALYZE provsql_test.r2_prime;
  ANALYZE provsql_test.r2_vboth;
  ANALYZE provsql_test.r2_vnone;
END;
$$;


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
    'dsharp',
    event_sql
  )
  INTO result_val;

  RETURN COALESCE(result_val, 0.0);
END;
$$;