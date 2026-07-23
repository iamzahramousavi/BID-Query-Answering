import csv
import re
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values


# ------------------------------------------------------------
# Settings
# ------------------------------------------------------------

DB_CONFIG = {
    "host": "localhost",
    "port": 55434,
    "dbname": "tutorial",
    "user": "test",
    "password": "YOUR_POSTGRESQL_PASSWORD",
}

R1_CSV = "/path/to/Pos_Edu_Exp_expanded.csv"
R2_CSV = "/path/to/Pos_Sb_Ds_expanded.csv"

# For the first ProvSQL aggregate test, use 100 blocks.
# Later, change this to None for the full data.
MAX_BLOCKS_PER_RELATION = None

# Optional: use exact block IDs instead of first N blocks.
# Example: SELECTED_BLOCK_IDS = ["1", "2", "3"]
SELECTED_BLOCK_IDS = None




def normalize_header(name: str) -> str:
    return str(name).strip().lower()


def numericish_sort_key(value: Any):
    s = str(value).strip()
    if re.fullmatch(r"-?\d+", s):
        return (0, int(s))
    return (1, s)


def parse_float(value: Any) -> float:
    """
    Robust float parser for values such as:
      150000
      150000.0
      $150,000
    """
    s = str(value).strip()
    s = s.replace("$", "").replace(",", "")
    if s == "":
        raise ValueError("Empty numeric value found.")
    return float(s)


def read_limited_csv_rows(
    csv_path: str,
    block_column: str,
    required_columns: List[str],
    max_blocks: Optional[int] = None,
    selected_block_ids: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
  
    block_col = normalize_header(block_column)
    required_cols = [normalize_header(c) for c in required_columns]

    selected_set = None
    if selected_block_ids is not None:
        selected_set = {str(x).strip() for x in selected_block_ids}

    selected_blocks_seen = OrderedDict()
    output_rows = []

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        if reader.fieldnames is None:
            raise ValueError(f"CSV file has no header row: {csv_path}")

        normalized_headers = {normalize_header(h) for h in reader.fieldnames}

        missing = set(required_cols) - normalized_headers
        if missing:
            raise ValueError(
                f"CSV file is missing required columns: {sorted(missing)}\n"
                f"File: {csv_path}\n"
                f"Found headers: {reader.fieldnames}"
            )

        for raw_row in reader:
            row = {
                normalize_header(k): "" if v is None else str(v).strip()
                for k, v in raw_row.items()
            }

            block_id = row[block_col]

            if selected_set is not None:
                if block_id not in selected_set:
                    continue
            elif max_blocks is not None:
                if block_id not in selected_blocks_seen:
                    if len(selected_blocks_seen) >= max_blocks:
                        continue
                    selected_blocks_seen[block_id] = True

            output_rows.append(row)

    return output_rows




def load_r1_rows() -> List[Tuple[str, str, str, str, float]]:
  
    required = ["block_id", "Education", "Experience", "Position", "probability"]

    rows = read_limited_csv_rows(
        csv_path=R1_CSV,
        block_column="block_id",
        required_columns=required,
        max_blocks=MAX_BLOCKS_PER_RELATION,
        selected_block_ids=SELECTED_BLOCK_IDS,
    )

    result = []
    for row in rows:
        result.append((
            row["block_id"],
            row["education"],
            row["experience"],
            row["position"],
            parse_float(row["probability"]),
        ))

    result.sort(
        key=lambda r: (
            numericish_sort_key(r[0]),
            r[1],
            r[2],
            r[3],
        )
    )

    return result


def load_r2_rows() -> List[Tuple[str, str, str, float, float]]:
 
    required = ["block_id", "Position", "SalaryBand", "DefaultSalary", "probability"]

    rows = read_limited_csv_rows(
        csv_path=R2_CSV,
        block_column="block_id",
        required_columns=required,
        max_blocks=MAX_BLOCKS_PER_RELATION,
        selected_block_ids=SELECTED_BLOCK_IDS,
    )

    result = []
    for row in rows:
        result.append((
            row["block_id"],
            row["position"],
            row["salaryband"],
            parse_float(row["defaultsalary"]),
            parse_float(row["probability"]),
        ))

    result.sort(
        key=lambda r: (
            numericish_sort_key(r[0]),
            r[1],
            r[2],
            r[3],
        )
    )

    return result




def main():
    r1_rows = load_r1_rows()
    r2_rows = load_r2_rows()

    r1_blocks = sorted({r[0] for r in r1_rows}, key=numericish_sort_key)
    r2_blocks = sorted({r[0] for r in r2_rows}, key=numericish_sort_key)

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS provsql_test;")

            cur.execute("DROP TABLE IF EXISTS provsql_test.R1p CASCADE;")
            cur.execute("DROP TABLE IF EXISTS provsql_test.R2p CASCADE;")

            cur.execute("""
                CREATE TABLE provsql_test.R1p (
                    block_id TEXT,
                    education TEXT,
                    experience TEXT,
                    position TEXT,
                    p1 DOUBLE PRECISION
                );
            """)

            cur.execute("""
                CREATE TABLE provsql_test.R2p (
                    block_id TEXT,
                    position TEXT,
                    salaryband TEXT,
                    defaultsalary DOUBLE PRECISION,
                    p2 DOUBLE PRECISION
                );
            """)

            if r1_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO provsql_test.R1p
                    (block_id, education, experience, position, p1)
                    VALUES %s
                    """,
                    r1_rows,
                    template="(%s, %s, %s, %s, %s)",
                )

            if r2_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO provsql_test.R2p
                    (block_id, position, salaryband, defaultsalary, p2)
                    VALUES %s
                    """,
                    r2_rows,
                    template="(%s, %s, %s, %s, %s)",
                )

            cur.execute("""
                CREATE INDEX idx_r1p_block
                ON provsql_test.R1p(block_id);
            """)

            cur.execute("""
                CREATE INDEX idx_r1p_position
                ON provsql_test.R1p(position);
            """)

            cur.execute("""
                CREATE INDEX idx_r1p_education
                ON provsql_test.R1p(education);
            """)

            cur.execute("""
                CREATE INDEX idx_r2p_block
                ON provsql_test.R2p(block_id);
            """)

            cur.execute("""
                CREATE INDEX idx_r2p_position
                ON provsql_test.R2p(position);
            """)

            cur.execute("""
                CREATE INDEX idx_r2p_salaryband
                ON provsql_test.R2p(salaryband);
            """)

            cur.execute("ANALYZE provsql_test.R1p;")
            cur.execute("ANALYZE provsql_test.R2p;")

            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT block_id)
                FROM provsql_test.R1p;
            """)
            r1_count, r1_block_count = cur.fetchone()

            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT block_id)
                FROM provsql_test.R2p;
            """)
            r2_count, r2_block_count = cur.fetchone()

            cur.execute("""
                SELECT MIN(defaultsalary), MAX(defaultsalary), AVG(defaultsalary)
                FROM provsql_test.R2p;
            """)
            min_ds, max_ds, avg_ds = cur.fetchone()

        conn.commit()

        print("CSV upload finished.")
        print()
        print("R1p:")
        print("  rows   =", r1_count)
        print("  blocks =", r1_block_count)
        print()
        print("R2p:")
        print("  rows   =", r2_count)
        print("  blocks =", r2_block_count)
        print("  min DefaultSalary =", min_ds)
        print("  max DefaultSalary =", max_ds)
        print("  avg DefaultSalary =", avg_ds)
        print()
        print("Selected R1p blocks:", r1_blocks[:10], "..." if len(r1_blocks) > 10 else "")
        print("Selected R2p blocks:", r2_blocks[:10], "..." if len(r2_blocks) > 10 else "")

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()