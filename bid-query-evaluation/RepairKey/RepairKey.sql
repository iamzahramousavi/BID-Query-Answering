SET search_path TO provsql_test, provsql, public;

DROP TABLE IF EXISTS provsql_test.repair_key_result CASCADE;
DROP TABLE IF EXISTS provsql_test.R1p CASCADE;
DROP TABLE IF EXISTS provsql_test.R2p CASCADE;

SELECT
    COUNT(*) AS r1_rows,
    COUNT(DISTINCT block_id) AS r1_blocks
FROM provsql_test.r1_bid_input;

SELECT
    COUNT(*) AS r2_rows,
    COUNT(DISTINCT block_id) AS r2_blocks
FROM provsql_test.r2_bid_input;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM provsql_test.r1_bid_input) THEN
        RAISE EXCEPTION 'provsql_test.r1_bid_input is empty. Run the repair-key Python CSV uploader first.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM provsql_test.r2_bid_input) THEN
        RAISE EXCEPTION 'provsql_test.r2_bid_input is empty. Run the repair-key Python CSV uploader first.';
    END IF;
END $$;

-- 1. Create repair-key copies of the original BID relations
CREATE TABLE provsql_test.R1p AS
SELECT
    block_id,
    education,
    experience,
    position,
    SUM(p)::double precision AS p1
FROM provsql_test.r1_bid_input
GROUP BY block_id, education, experience, position;

CREATE TABLE provsql_test.R2p AS
SELECT
    block_id,
    position,
    salaryband,
    SUM(p)::double precision AS p2
FROM provsql_test.r2_bid_input
GROUP BY block_id, position, salaryband;

-- 2. Apply repair_key to enforce exactly-one tuple per block
SELECT repair_key('R1p', 'block_id');
SELECT repair_key('R2p', 'block_id');

-- 3. Assign the original BID probabilities
DO $$
BEGIN
  PERFORM set_prob(provenance(), p1)
  FROM provsql_test.R1p;

  PERFORM set_prob(provenance(), p2)
  FROM provsql_test.R2p;
END $$;

-- 4. Evaluate the same Boolean join query
CREATE TABLE provsql_test.repair_key_result AS
SELECT
    *,
    probability_evaluate(provenance(), 'compilation', 'd4') AS prob
FROM (
    SELECT TRUE AS k
    FROM provsql_test.R1p r1
    JOIN provsql_test.R2p r2
      ON r1.position = r2.position
    WHERE r1.education = 'PhD'
      AND r1.experience = '0-15'
      AND r2.salaryband = '121k+'
    GROUP BY k
) q;

-- 5. Show the repair-key result
SELECT
    k,
    prob AS repair_key_probability,
    prob::numeric(30, 20) AS repair_key_probability_20_digits
FROM provsql_test.repair_key_result;

-- 6. Optional: remove provenance from result table
SELECT remove_provenance('repair_key_result');