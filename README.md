# Probabilistic Query Evaluation over BID Databases using the MarkoViews Approach and ProvSQL

This repository contains the implementation developed for my Master's thesis on probabilistic query evaluation over Block-Independent-Disjoint probabilistic Databases (BID).

The implementation evaluates Boolean queries over BID databases using the MarkoViews approach with ProvSQL as the probability-evaluation engine. It also includes implementations based on the ProvSQL `repair_key` operator, Query-Dependent Restricted MarkoViews, and an aggregate-query experiment.

---

# Repository Structure

```text
bid-query-evaluation/
│
├── aggregate_query/
│   ├── aggregate_query.py
│   └── aggregate_provSQL.sql
│
├── data/
│   ├── 30_blocks/
│   │   ├── eid_education_experience_position.csv
│   │   └── eid_position_salaryband_defaultsalary.csv
│   │
│   ├── 50_blocks/
│   │   ├── eid_education_experience_position.csv
│   │   └── eid_position_salaryband_defaultsalary.csv
│   │
│   ├── 70_blocks/
│   │   ├── eid_education_experience_position.csv
│   │   └── eid_position_salaryband_defaultsalary.csv
│   │
│   ├── 100_blocks/
│   │   ├── eid_education_experience_position.csv
│   │   └── eid_position_salaryband_defaultsalary.csv
│   │
│   └── 5000_blocks/
│       ├── eid_education_experience_position.csv
│       └── eid_position_salaryband_defaultsalary.csv
│
├── fullMarkoviews/
│   ├── Full_MarkoViews.py
│   └── Full_MarkoViews.sql
│
├── QueryDependentRestrictedMarkoViews/
│   ├── QueryDependentRestrictedMarkoViews.py
│   └── QueryDependentRestrictedMarkoViews.sql
│
├── RepairKey/
│   ├── Repair_key.py
│   └── RepairKey.sql
│
└── testresult_example.sql
```

---

# Repository Contents

## fullMarkoviews

Implementation of the Full MarkoViews approach.

The Python program constructs the auxiliary Tuple-Independent probabilistic Database (TID), generates the violation formulas, executes the corresponding provenance queries in PostgreSQL/ProvSQL, retrieves the auxiliary probabilities, and computes the final BID probability.

For each BID tuple with probability `p`, the corresponding TID probability is:

```text
p' = p / (1 + p)
```

The violation event `U` represents violations of the exactly-one constraint of the BID blocks.

To avoid numerical cancellation when `P1(U)` is extremely close to `1`, the implementation directly evaluates:

```text
P0(Q) = P1(Q | not U) = P1(Q and not U) / P1(not U)
```

Knowledge compilation is performed through ProvSQL using either `d4` or `dsharp`.

---

## QueryDependentRestrictedMarkoViews

Implementation of the Query-Dependent Restricted MarkoViews approach used in the experimental study.

The method identifies query-relevant blocks and `eid` values and evaluates smaller block-level MarkoViews events using ProvSQL.

The block-level probabilities are then combined to obtain the final BID query probability.

Knowledge compilation can be performed using either `d4` or `dsharp`.

---

## RepairKey

Implementation based on the `repair_key` operator provided by ProvSQL.

The `repair_key` operator is used to represent the mutually exclusive alternatives within each BID block.

The same Boolean query can be evaluated using:

- ProvSQL possible-world evaluation;
- knowledge compilation with `d4`;
- knowledge compilation with `dsharp`.

---

## aggregate_query

Implementation of the aggregate-query experiment.

The experiment computes the average `DefaultSalary` for tuples satisfying the specified query conditions after joining the two relations using `eid`.

---

## data

The datasets used in the experiments.

```text
data/
├── 30_blocks/
├── 50_blocks/
├── 70_blocks/
├── 100_blocks/
└── 5000_blocks/
```

Each dataset contains two relations:

```text
eid_education_experience_position.csv
eid_position_salaryband_defaultsalary.csv
```

The first relation contains:

```text
block_id
eid
Education
Experience
Position
probability
```

The second relation contains:

```text
block_id
eid
Position
SalaryBand
DefaultSalary
probability
```

The two relations are joined using:

```sql
r1.eid = r2.eid
```

`Position` is an ordinary attribute and is not used as the join attribute.

---

# Boolean Query

The Boolean conjunctive query used in the Full MarkoViews, Query-Dependent Restricted MarkoViews, and Repair Key experiments is:

```sql
SELECT TRUE AS k
FROM R1 r1
JOIN R2 r2
  ON r1.eid = r2.eid
WHERE r1.Education = 'PhD'
  AND r1.Experience = '0-15'
  AND r1.Position = 'Senior'
  AND r2.SalaryBand = '121k+';
```

The join condition is:

```sql
r1.eid = r2.eid
```

while

```sql
r1.Position = 'Senior'
```

is a selection condition.

---

# Aggregate Query

The aggregate-query experiment evaluates:

```sql
SELECT
    AVG(r2.defaultsalary)::numeric AS q2_avg_default_salary
FROM R1p r1
JOIN R2p r2
  ON r1.eid = r2.eid
WHERE r1.education = 'PhD'
  AND r1.experience = '15-30+'
  AND r1.position = 'Manager'
  AND r2.salaryband = '121k+';
```

The aggregate query was evaluated over the 30-, 50-, 70-, 100-, and 5000-block datasets.

---

# Requirements

The implementation was developed using:

- Python 3
- PostgreSQL
- Docker Desktop
- ProvSQL 1.12.0

Install the Python dependencies with:

```bash
pip install -r requirements.txt
```

The Python implementations use `psycopg2` to communicate with PostgreSQL and ProvSQL.

---

# Installing ProvSQL

The experiments use ProvSQL version 1.12.0.

Official ProvSQL repository:

https://github.com/PierreSenellart/provsql

Docker Desktop:

https://docs.docker.com/desktop/

The ProvSQL Docker image used for the experiments is:

```text
inriavalda/provsql:1.12.0
```

An example command for starting the experimental container is:

```bash
docker run -d \
  --name provsql112 \
  --platform linux/amd64 \
  -p 55435:5432 \
  -p 8003:8000 \
  inriavalda/provsql:1.12.0
```

This configuration uses:

- PostgreSQL host port: **55435**
- ProvSQL web interface port: **8003**
- Database: **tutorial**
- User: **test**

Verify that the container is running:

```bash
docker ps
```

Connect to PostgreSQL:

```bash
psql -h localhost -p 55435 -d tutorial -U test
```

Alternatively, pgAdmin may be used with:

```text
Host: localhost
Port: 55435
Database: tutorial
Username: test
```

---

# Configuration

The Python programs use the following PostgreSQL/ProvSQL configuration:

```python
DB_CONFIG = {
    "host": "localhost",
    "port": 55435,
    "dbname": "tutorial",
    "user": "test",
}
```

The public Python implementations do not contain machine-specific dataset paths.

Instead, the R1 and R2 CSV files are supplied when running the Python program.

For example:

```bash
python Repair_key.py \
  --r1 "/path/to/eid_education_experience_position.csv" \
  --r2 "/path/to/eid_position_salaryband_defaultsalary.csv"
```

---

# Running the Experiments

Each implementation consists of a SQL file and a Python program.

The general workflow is:

1. Start the ProvSQL 1.12.0 Docker container.
2. Connect to the `tutorial` PostgreSQL database.
3. Execute the SQL script corresponding to the selected approach.
4. Run the corresponding Python program with the R1 and R2 CSV paths.
5. Select `d4` or `dsharp` in the SQL probability-evaluation function when required.

For Full MarkoViews:

```bash
python Full_MarkoViews.py \
  --r1 "/path/to/eid_education_experience_position.csv" \
  --r2 "/path/to/eid_position_salaryband_defaultsalary.csv"
```

For Query-Dependent Restricted MarkoViews:

```bash
python QueryDependentRestrictedMarkoViews.py \
  --r1 "/path/to/eid_education_experience_position.csv" \
  --r2 "/path/to/eid_position_salaryband_defaultsalary.csv"
```

For Repair Key:

```bash
python Repair_key.py \
  --r1 "/path/to/eid_education_experience_position.csv" \
  --r2 "/path/to/eid_position_salaryband_defaultsalary.csv"
```

The aggregate-query uploader uses the same command-line pattern.

---

# Knowledge Compilation

ProvSQL is used as the probability-evaluation engine.

For knowledge compilation with `d4`:

```sql
'compilation',
'd4'
```

For knowledge compilation with `dsharp`:

```sql
'compilation',
'dsharp'
```

The Python implementation remains unchanged when switching between the two knowledge compilers.

---

# Scalability Notes

The Full MarkoViews and Query-Dependent Restricted MarkoViews approaches were evaluated with `d4` and `dsharp` on the smaller datasets.

For the full 5000-block dataset, Full MarkoViews knowledge compilation did not complete with either `d4` or `dsharp`; ProvSQL reported compiler termination with status 137.

The Query-Dependent Restricted MarkoViews evaluation over the 5000-block dataset also did not complete under the available system memory/resources, and therefore no probability was reported for that configuration.

---

# Example Query

`testresult_example.sql` contains the diagnostic example used to validate the probabilistic query-evaluation implementation.

---

# Notes

This repository accompanies the implementation described in the Master's thesis.

It is intended to support research reproducibility and documentation of the experimental implementation rather than to serve as a general-purpose software package.
