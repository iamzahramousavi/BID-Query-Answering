# Probabilistic Query Evaluation over BID Databases using the MarkoViews Approach and ProvSQL

This repository contains the implementation developed for my Master's thesis on probabilistic query evaluation over Block-Independent Disjoint (BID) databases.

The implementation evaluates Boolean queries over BID databases using the MarkoViews approach with ProvSQL as the probability-evaluation engine. It also includes implementations based on the ProvSQL `repair_key` operator, a restricted MarkoViews evaluation, and aggregate-query experiments.

---

# Repository Structure

```
bid-query-evaluation//
│
├── aggregate_query/
│   ├── aggregate_query.py
│   └── aggregate_provSQL1.9.sql
│
├── data/
│   ├── full_ordinary/
│   └── reduced/
│
├── fullMarkoviews/
│   ├── Full_MarkoViews.py
│   └── Full_MarkoViews.sql
│
├── RepairKey/
│   ├── Repair_key.py
│   └── RepairKey.sql
│
├── Restricted/
│   ├── Restricted.py
│   └── Restricted.sql
│
└── testresult_example.sql
```

---

# Repository Contents

## fullMarkoviews

Implementation of the complete MarkoViews approach.

The Python program constructs the auxiliary TID database, generates the violation formulas, executes the SQL queries in PostgreSQL/ProvSQL, retrieves the auxiliary probabilities, and computes the final BID probability.

---

## Restricted

Implementation of the restricted (query-relevant block) MarkoViews evaluation used in the experimental study.

---

## RepairKey

Implementation based on the `repair_key` operator provided by ProvSQL.

This implementation evaluates the same Boolean query without explicitly constructing the MarkoViews violation formula.

---

## aggregate_query

Implementation of the aggregate-query experiments using ProvSQL.

---

## data

The datasets used in the experiments.

```
data/
├── full_ordinary/
└── reduced/
```

`full_ordinary` contains the complete BID datasets used in the full experiments.

`reduced` contains the smaller datasets used in the scalability experiments (30, 50, 70, 100, and 5000 blocks).

---

# Requirements

The implementation was developed using

- Python 3
- PostgreSQL
- Docker Desktop
- ProvSQL

Python dependency:

```
pip install psycopg2-binary
```

---

# Installing ProvSQL

The experiments use the official ProvSQL Docker image.

Official ProvSQL repository:

https://github.com/PierreSenellart/provsql

Install Docker Desktop:

https://docs.docker.com/desktop/

Start the ProvSQL container:

```bash
docker run -d -p 55433:5432 -p 8081:80 inriavalda/provsql
```

This starts

- PostgreSQL on port **55433**
- ProvSQL web interface on port **8081**

Verify that the container is running:

```bash
docker ps
```

Connect to PostgreSQL:

```bash
psql -h localhost -p 55433 -d test -U postgres
```

Alternatively, pgAdmin may be used to connect to the Docker container by specifying

- Host: localhost
- Port: 55433
- Database: test
- Username: postgres

The Docker setup follows the configuration used in the thesis experiments. :contentReference[oaicite:1]{index=1}

---

# Configuration

Before running the programs, update the PostgreSQL connection information in the Python files.

Replace

```python
DB_CONFIG = {
    ...
}
```

with your local PostgreSQL credentials.

Also update the CSV paths so that they point to the datasets stored on your machine.

---

# Running the Experiments

Each implementation consists of a SQL file and a Python program.

The general workflow is

1. Start the ProvSQL Docker container.
2. Connect to PostgreSQL.
3. Execute the corresponding SQL script.
4. Update the dataset path and database credentials in the Python file.
5. Execute the Python program.

---

# Example Query

`testresult_example.sql`

contains an example Boolean query used during testing.

---

# Notes

This repository accompanies the implementation described in the Master's thesis.

It is intended for research reproducibility rather than as a general-purpose software package.
