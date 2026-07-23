SELECT current_database(), current_user;
SELECT extversion
FROM pg_extension
WHERE extname = 'provsql';

SET search_path TO provsql_test, public, provsql;

SELECT repair_key('R1p', 'block_id');
SELECT repair_key('R2p', 'block_id');

SELECT set_prob(provenance(), p1)
FROM R1p;

SELECT set_prob(provenance(), p2)
FROM R2p;

SELECT
    probability_evaluate(provenance()) AS p_max_default_salary_at_least_150000
FROM R1p r1
JOIN R2p r2
  ON r1.position = r2.position
WHERE r1.education = 'PhD'
HAVING MAX(r2.defaultsalary) >= 150000;

-----------non probabilistic query, just aggregate 
SET search_path TO provsql_test, public, provsql;

-- 1. Check that you are in the correct ProvSQL version

SELECT current_database(), current_user;

SELECT extversion
FROM pg_extension
WHERE extname = 'provsql';


-- 2. Check that the uploaded tables exist

SELECT COUNT(*) AS r1_rows
FROM R1p;

SELECT COUNT(*) AS r2_rows
FROM R2p;


-- 3. Apply repair-key semantics to the BID blocks

SELECT repair_key('R1p', 'block_id');
SELECT repair_key('R2p', 'block_id');


-- 4. Assign tuple probabilities

SELECT set_prob(provenance(), p1)
FROM R1p;

SELECT set_prob(provenance(), p2)
FROM R2p;


-- 5. Normal aggregate query using repair-key tables

SELECT MAX(r2.defaultsalary) AS max_default_salary
FROM R1p r1
JOIN R2p r2
  ON r1.position = r2.position
WHERE r1.education = 'PhD';

------AVG query
SET search_path TO provsql_test, public, provsql;

-- Apply repair-key semantics to the BID blocks.
SELECT repair_key('R1p', 'block_id');
SELECT repair_key('R2p', 'block_id');

-- Assign tuple probabilities.
SELECT set_prob(provenance(), p1)
FROM R1p;

SELECT set_prob(provenance(), p2)
FROM R2p;

-- Normal AVG aggregate query under repair-key semantics.
SELECT AVG(r2.defaultsalary) AS avg_default_salary
FROM R1p r1
JOIN R2p r2
  ON r1.position = r2.position
WHERE r1.education = 'PhD';