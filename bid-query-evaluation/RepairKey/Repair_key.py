import argparse
import csv
import re
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values


# ============================================================
# Database configuration
#
# ProvSQL 1.12.0
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 55435,
    "dbname": "tutorial",
    "user": "test",
}


# ============================================================
# Optional block restriction
#
# None means that all blocks in the supplied CSV files are used.
#
# Example:
#     MAX_BLOCKS_PER_RELATION = 100
#
# Optional exact block selection:
#     SELECTED_BLOCK_IDS = ["1", "2", "3"]
# ============================================================

MAX_BLOCKS_PER_RELATION = None

SELECTED_BLOCK_IDS = None


# ============================================================
# Utility functions
# ============================================================

def normalize_header(name: str) -> str:
    return str(name).strip().lower()


def numericish_sort_key(value: Any):
    """
    Sort numeric-looking strings numerically and all other
    values lexicographically.
    """

    s = str(value).strip()

    if re.fullmatch(r"-?\d+", s):
        return 0, int(s)

    return 1, s


def parse_float(value: Any) -> float:
    """
    Parse numeric values such as:

        0.8
        150000
        150000.0
        $150,000
    """

    s = str(value).strip()

    s = (
        s
        .replace("$", "")
        .replace(",", "")
    )

    if s == "":
        raise ValueError(
            "Empty numeric value found."
        )

    return float(s)


# ============================================================
# CSV reader
# ============================================================

def read_limited_csv_rows(
    csv_path: str,
    block_column: str,
    required_columns: List[str],
    max_blocks: Optional[int] = None,
    selected_block_ids: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    """
    Read rows from a BID CSV.

    If selected_block_ids is provided, only those blocks are
    retained.

    Otherwise, if max_blocks is provided, only the first
    max_blocks distinct blocks are retained.

    Otherwise, the complete CSV file is read.
    """

    block_col = normalize_header(
        block_column
    )

    required_cols = [
        normalize_header(c)
        for c in required_columns
    ]

    selected_set = None

    if selected_block_ids is not None:
        selected_set = {
            str(x).strip()
            for x in selected_block_ids
        }

    selected_blocks_seen = OrderedDict()

    output_rows = []

    with open(
        csv_path,
        newline="",
        encoding="utf-8-sig"
    ) as f:

        reader = csv.DictReader(f)

        if reader.fieldnames is None:
            raise ValueError(
                f"CSV file has no header row: {csv_path}"
            )

        normalized_headers = {
            normalize_header(h)
            for h in reader.fieldnames
        }

        missing = (
            set(required_cols)
            -
            normalized_headers
        )

        if missing:
            raise ValueError(
                "CSV file is missing required columns.\n"
                f"Missing: {sorted(missing)}\n"
                f"File: {csv_path}\n"
                f"Found headers: {reader.fieldnames}"
            )

        for raw_row in reader:

            row = {
                normalize_header(k):
                    (
                        ""
                        if v is None
                        else str(v).strip()
                    )
                for k, v in raw_row.items()
            }

            block_id = row[
                block_col
            ]

            # ------------------------------------------------
            # Exact block selection
            # ------------------------------------------------

            if selected_set is not None:

                if block_id not in selected_set:
                    continue

            # ------------------------------------------------
            # First N blocks
            # ------------------------------------------------

            elif max_blocks is not None:

                if (
                    block_id
                    not in selected_blocks_seen
                ):

                    if (
                        len(selected_blocks_seen)
                        >= max_blocks
                    ):
                        continue

                    selected_blocks_seen[
                        block_id
                    ] = True

            output_rows.append(
                row
            )

    return output_rows


# ============================================================
# Load R1
#
# CSV schema:
#
# block_id
# eid
# Education
# Experience
# Position
# probability
# ============================================================

def load_r1_rows(
    csv_path: str
) -> List[
    Tuple[
        str,
        str,
        str,
        str,
        str,
        float
    ]
]:

    required = [
        "block_id",
        "eid",
        "Education",
        "Experience",
        "Position",
        "probability",
    ]

    rows = read_limited_csv_rows(
        csv_path=
            csv_path,

        block_column=
            "block_id",

        required_columns=
            required,

        max_blocks=
            MAX_BLOCKS_PER_RELATION,

        selected_block_ids=
            SELECTED_BLOCK_IDS,
    )

    result = []

    for row in rows:

        result.append(
            (
                row["block_id"],
                row["eid"],
                row["education"],
                row["experience"],
                row["position"],
                parse_float(
                    row["probability"]
                ),
            )
        )

    result.sort(
        key=lambda r: (
            numericish_sort_key(
                r[0]
            ),
            numericish_sort_key(
                r[1]
            ),
            r[2],
            r[3],
            r[4],
        )
    )

    return result


# ============================================================
# Load R2
#
# CSV schema:
#
# block_id
# eid
# Position
# SalaryBand
# DefaultSalary
# probability
# ============================================================

def load_r2_rows(
    csv_path: str
) -> List[
    Tuple[
        str,
        str,
        str,
        str,
        float,
        float
    ]
]:

    required = [
        "block_id",
        "eid",
        "Position",
        "SalaryBand",
        "DefaultSalary",
        "probability",
    ]

    rows = read_limited_csv_rows(
        csv_path=
            csv_path,

        block_column=
            "block_id",

        required_columns=
            required,

        max_blocks=
            MAX_BLOCKS_PER_RELATION,

        selected_block_ids=
            SELECTED_BLOCK_IDS,
    )

    result = []

    for row in rows:

        result.append(
            (
                row["block_id"],
                row["eid"],
                row["position"],
                row["salaryband"],
                parse_float(
                    row["defaultsalary"]
                ),
                parse_float(
                    row["probability"]
                ),
            )
        )

    result.sort(
        key=lambda r: (
            numericish_sort_key(
                r[0]
            ),
            numericish_sort_key(
                r[1]
            ),
            r[2],
            r[3],
            r[4],
        )
    )

    return result


# ============================================================
# Upload BID relations to PostgreSQL
# ============================================================

def upload_to_postgresql(
    r1_csv: str,
    r2_csv: str
):

    print(
        f"\nReading R1 from:\n{r1_csv}"
    )

    r1_rows = load_r1_rows(
        r1_csv
    )

    print(
        f"\nReading R2 from:\n{r2_csv}"
    )

    r2_rows = load_r2_rows(
        r2_csv
    )

    conn = psycopg2.connect(
        **DB_CONFIG
    )

    try:

        with conn.cursor() as cur:

            # =================================================
            # Check database and ProvSQL version
            # =================================================

            cur.execute(
                """
                SELECT
                    current_database(),
                    current_user,
                    (
                        SELECT extversion
                        FROM pg_extension
                        WHERE extname = 'provsql'
                    );
                """
            )

            database_name, user_name, provsql_version = (
                cur.fetchone()
            )

            print(
                "\nConnected to:"
            )

            print(
                "  database       =",
                database_name
            )

            print(
                "  user           =",
                user_name
            )

            print(
                "  ProvSQL version =",
                provsql_version
            )

            if database_name != "tutorial":
                raise RuntimeError(
                    "Wrong database. "
                    "Expected 'tutorial'."
                )

            if provsql_version != "1.12.0":
                raise RuntimeError(
                    "Wrong ProvSQL version. "
                    "Expected 1.12.0."
                )

            # =================================================
            # Schema
            # =================================================

            cur.execute(
                """
                CREATE SCHEMA
                IF NOT EXISTS provsql_test;
                """
            )

            cur.execute(
                """
                SET search_path TO
                    provsql_test,
                    public,
                    provsql;
                """
            )

            # =================================================
            # Drop previous input tables
            # =================================================

            cur.execute(
                """
                DROP TABLE IF EXISTS
                    provsql_test.r1_bid_input
                CASCADE;
                """
            )

            cur.execute(
                """
                DROP TABLE IF EXISTS
                    provsql_test.r2_bid_input
                CASCADE;
                """
            )

            # =================================================
            # R1 input relation
            # =================================================

            cur.execute(
                """
                CREATE TABLE
                provsql_test.r1_bid_input
                (
                    block_id TEXT,
                    eid TEXT,
                    education TEXT,
                    experience TEXT,
                    position TEXT,
                    p DOUBLE PRECISION
                );
                """
            )

            # =================================================
            # R2 input relation
            # =================================================

            cur.execute(
                """
                CREATE TABLE
                provsql_test.r2_bid_input
                (
                    block_id TEXT,
                    eid TEXT,
                    position TEXT,
                    salaryband TEXT,
                    defaultsalary DOUBLE PRECISION,
                    p DOUBLE PRECISION
                );
                """
            )

            # =================================================
            # Insert R1
            # =================================================

            if r1_rows:

                execute_values(
                    cur,

                    """
                    INSERT INTO
                    provsql_test.r1_bid_input
                    (
                        block_id,
                        eid,
                        education,
                        experience,
                        position,
                        p
                    )
                    VALUES %s
                    """,

                    r1_rows,

                    template=
                        "(%s, %s, %s, %s, %s, %s)",
                )

            # =================================================
            # Insert R2
            # =================================================

            if r2_rows:

                execute_values(
                    cur,

                    """
                    INSERT INTO
                    provsql_test.r2_bid_input
                    (
                        block_id,
                        eid,
                        position,
                        salaryband,
                        defaultsalary,
                        p
                    )
                    VALUES %s
                    """,

                    r2_rows,

                    template=
                        "(%s, %s, %s, %s, %s, %s)",
                )

            # =================================================
            # Useful indexes
            # =================================================

            cur.execute(
                """
                CREATE INDEX idx_r1_bid_block
                ON provsql_test.r1_bid_input(block_id);
                """
            )

            cur.execute(
                """
                CREATE INDEX idx_r1_bid_eid
                ON provsql_test.r1_bid_input(eid);
                """
            )

            cur.execute(
                """
                CREATE INDEX idx_r2_bid_block
                ON provsql_test.r2_bid_input(block_id);
                """
            )

            cur.execute(
                """
                CREATE INDEX idx_r2_bid_eid
                ON provsql_test.r2_bid_input(eid);
                """
            )

            cur.execute(
                """
                ANALYZE provsql_test.r1_bid_input;
                """
            )

            cur.execute(
                """
                ANALYZE provsql_test.r2_bid_input;
                """
            )

            # =================================================
            # Verify counts
            # =================================================

            cur.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT block_id),
                    COUNT(DISTINCT eid)
                FROM provsql_test.r1_bid_input;
                """
            )

            (
                r1_count,
                r1_block_count,
                r1_eid_count
            ) = cur.fetchone()

            cur.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT block_id),
                    COUNT(DISTINCT eid)
                FROM provsql_test.r2_bid_input;
                """
            )

            (
                r2_count,
                r2_block_count,
                r2_eid_count
            ) = cur.fetchone()

            # =================================================
            # Verify that the eid join is possible
            # =================================================

            cur.execute(
                """
                SELECT COUNT(*)
                FROM provsql_test.r1_bid_input r1
                JOIN provsql_test.r2_bid_input r2
                  ON r1.eid = r2.eid;
                """
            )

            eid_join_rows = (
                cur.fetchone()[0]
            )

        conn.commit()

        # ====================================================
        # Summary
        # ====================================================

        print(
            "\nCSV upload finished."
        )

        print(
            "\nR1:"
        )

        print(
            "  rows   =",
            r1_count
        )

        print(
            "  blocks =",
            r1_block_count
        )

        print(
            "  eids   =",
            r1_eid_count
        )

        print(
            "\nR2:"
        )

        print(
            "  rows   =",
            r2_count
        )

        print(
            "  blocks =",
            r2_block_count
        )

        print(
            "  eids   =",
            r2_eid_count
        )

        print(
            "\nJoin check:"
        )

        print(
            "  rows matching R1.eid = R2.eid =",
            eid_join_rows
        )

    except Exception:

        conn.rollback()

        raise

    finally:

        conn.close()


# ============================================================
# Command-line interface
# ============================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Upload the R1 and R2 BID relations used by "
            "the ProvSQL repair_key experiment."
        )
    )

    parser.add_argument(
        "--r1",
        required=True,
        help=(
            "Path to "
            "eid_education_experience_position.csv"
        ),
    )

    parser.add_argument(
        "--r2",
        required=True,
        help=(
            "Path to "
            "eid_position_salaryband_defaultsalary.csv"
        ),
    )

    return parser.parse_args()


# ============================================================
# Main
# ============================================================

def main():

    args = parse_arguments()

    r1_path = Path(
        args.r1
    ).expanduser().resolve()

    r2_path = Path(
        args.r2
    ).expanduser().resolve()

    if not r1_path.is_file():
        raise FileNotFoundError(
            f"R1 CSV was not found:\n{r1_path}"
        )

    if not r2_path.is_file():
        raise FileNotFoundError(
            f"R2 CSV was not found:\n{r2_path}"
        )

    upload_to_postgresql(
        r1_csv=
            str(r1_path),

        r2_csv=
            str(r2_path),
    )


if __name__ == "__main__":

    main()
