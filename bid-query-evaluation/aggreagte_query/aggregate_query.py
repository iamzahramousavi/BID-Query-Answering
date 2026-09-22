import argparse
import csv
import re

from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values


# ============================================================
# Aggregate-query experiment
#
# ProvSQL version: 1.12.0
#
# The input BID relations are:
#
# R1:
#   block_id
#   eid
#   Education
#   Experience
#   Position
#   probability
#
# R2:
#   block_id
#   eid
#   Position
#   SalaryBand
#   DefaultSalary
#   probability
#
# The aggregate query is evaluated in aggregate_query.sql.
#
# The CSV paths are supplied through command-line arguments so
# that this script does not depend on machine-specific paths.
# ============================================================


# ============================================================
# DATABASE CONFIGURATION
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 55435,
    "dbname": "tutorial",
    "user": "test",
    "password": "test",
}


# ============================================================
# OPTIONAL BLOCK SELECTION
#
# None means that all blocks in the supplied CSV files are used.
#
# Example:
#
# MAX_BLOCKS_PER_RELATION = 100
#
# or:
#
# SELECTED_BLOCK_IDS = ["1", "2", "3"]
# ============================================================

MAX_BLOCKS_PER_RELATION = None

SELECTED_BLOCK_IDS = None


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def normalize_header(name: str) -> str:
    return str(name).strip().lower()


def numericish_sort_key(value: Any):
    """
    Sort numeric-looking values numerically and all other
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


def parse_int(value: Any) -> int:
    """
    Parse integer-like values such as:

        17
        17.0
    """

    s = str(value).strip()

    if s == "":
        raise ValueError(
            "Empty integer value found."
        )

    return int(
        float(s)
    )


# ============================================================
# CSV READER
# ============================================================

def read_limited_csv_rows(
    csv_path: str,
    block_column: str,
    required_columns: List[str],
    max_blocks: Optional[int] = None,
    selected_block_ids: Optional[List[str]] = None,
) -> List[Dict[str, str]]:

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

        reader = csv.DictReader(
            f
        )

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
                f"Headers: {reader.fieldnames}"
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
            # First N distinct blocks
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
# LOAD R1
#
# CSV columns:
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
        int,
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
                row[
                    "block_id"
                ],

                parse_int(
                    row[
                        "eid"
                    ]
                ),

                row[
                    "education"
                ],

                row[
                    "experience"
                ],

                row[
                    "position"
                ],

                parse_float(
                    row[
                        "probability"
                    ]
                ),
            )
        )

    result.sort(
        key=lambda r: (
            numericish_sort_key(
                r[0]
            ),
            r[1],
            r[2],
            r[3],
            r[4],
        )
    )

    return result


# ============================================================
# LOAD R2
#
# CSV columns:
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
        int,
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
                row[
                    "block_id"
                ],

                parse_int(
                    row[
                        "eid"
                    ]
                ),

                row[
                    "position"
                ],

                row[
                    "salaryband"
                ],

                parse_float(
                    row[
                        "defaultsalary"
                    ]
                ),

                parse_float(
                    row[
                        "probability"
                    ]
                ),
            )
        )

    result.sort(
        key=lambda r: (
            numericish_sort_key(
                r[0]
            ),
            r[1],
            r[2],
            r[3],
            r[4],
        )
    )

    return result


# ============================================================
# UPLOAD BID RELATIONS
# ============================================================

def upload_relations(
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


    r1_blocks = sorted(
        {
            row[0]
            for row in r1_rows
        },
        key=
            numericish_sort_key
    )


    r2_blocks = sorted(
        {
            row[0]
            for row in r2_rows
        },
        key=
            numericish_sort_key
    )


    conn = psycopg2.connect(
        **DB_CONFIG
    )


    try:

        with conn.cursor() as cur:

            # =================================================
            # VERIFY DATABASE ENVIRONMENT
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

            info = cur.fetchone()

            print(
                "\nConnected as:",
                info
            )

            if info[0] != "tutorial":

                raise RuntimeError(
                    "Wrong database. "
                    "Expected 'tutorial'."
                )

            if info[2] != "1.12.0":

                raise RuntimeError(
                    "Wrong ProvSQL version. "
                    "Expected 1.12.0."
                )


            # =================================================
            # SCHEMA
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
            # DROP PREVIOUS TABLES
            # =================================================

            cur.execute(
                """
                DROP TABLE IF EXISTS
                    provsql_test.R1p
                CASCADE;
                """
            )


            cur.execute(
                """
                DROP TABLE IF EXISTS
                    provsql_test.R2p
                CASCADE;
                """
            )


            # =================================================
            # CREATE R1p
            # =================================================

            cur.execute(
                """
                CREATE TABLE
                provsql_test.R1p
                (
                    block_id TEXT,
                    eid INTEGER,
                    education TEXT,
                    experience TEXT,
                    position TEXT,
                    p1 DOUBLE PRECISION
                );
                """
            )


            # =================================================
            # CREATE R2p
            # =================================================

            cur.execute(
                """
                CREATE TABLE
                provsql_test.R2p
                (
                    block_id TEXT,
                    eid INTEGER,
                    position TEXT,
                    salaryband TEXT,
                    defaultsalary DOUBLE PRECISION,
                    p2 DOUBLE PRECISION
                );
                """
            )


            # =================================================
            # INSERT R1
            # =================================================

            if r1_rows:

                execute_values(
                    cur,

                    """
                    INSERT INTO
                    provsql_test.R1p
                    (
                        block_id,
                        eid,
                        education,
                        experience,
                        position,
                        p1
                    )
                    VALUES %s
                    """,

                    r1_rows,

                    template=
                        "(%s, %s, %s, %s, %s, %s)",
                )


            # =================================================
            # INSERT R2
            # =================================================

            if r2_rows:

                execute_values(
                    cur,

                    """
                    INSERT INTO
                    provsql_test.R2p
                    (
                        block_id,
                        eid,
                        position,
                        salaryband,
                        defaultsalary,
                        p2
                    )
                    VALUES %s
                    """,

                    r2_rows,

                    template=
                        "(%s, %s, %s, %s, %s, %s)",
                )


            # =================================================
            # INDEXES
            # =================================================

            cur.execute(
                """
                CREATE INDEX idx_r1p_block
                ON provsql_test.R1p(block_id);
                """
            )


            cur.execute(
                """
                CREATE INDEX idx_r1p_eid
                ON provsql_test.R1p(eid);
                """
            )


            cur.execute(
                """
                CREATE INDEX idx_r1p_query
                ON provsql_test.R1p
                (
                    eid,
                    education,
                    experience,
                    position
                );
                """
            )


            cur.execute(
                """
                CREATE INDEX idx_r2p_block
                ON provsql_test.R2p(block_id);
                """
            )


            cur.execute(
                """
                CREATE INDEX idx_r2p_eid
                ON provsql_test.R2p(eid);
                """
            )


            cur.execute(
                """
                CREATE INDEX idx_r2p_query
                ON provsql_test.R2p
                (
                    eid,
                    salaryband
                );
                """
            )


            # =================================================
            # ANALYZE
            # =================================================

            cur.execute(
                """
                ANALYZE provsql_test.R1p;
                """
            )


            cur.execute(
                """
                ANALYZE provsql_test.R2p;
                """
            )


            # =================================================
            # VERIFY COUNTS
            # =================================================

            cur.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT block_id),
                    COUNT(DISTINCT eid)
                FROM provsql_test.R1p;
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
                FROM provsql_test.R2p;
                """
            )

            (
                r2_count,
                r2_block_count,
                r2_eid_count
            ) = cur.fetchone()


            # =================================================
            # VERIFY eid JOIN
            # =================================================

            cur.execute(
                """
                SELECT COUNT(*)
                FROM provsql_test.R1p r1
                JOIN provsql_test.R2p r2
                  ON r1.eid = r2.eid;
                """
            )

            eid_join_count = (
                cur.fetchone()[0]
            )


            # =================================================
            # SALARY SUMMARY
            # =================================================

            cur.execute(
                """
                SELECT
                    MIN(defaultsalary),
                    MAX(defaultsalary),
                    AVG(defaultsalary)
                FROM provsql_test.R2p;
                """
            )

            (
                min_salary,
                max_salary,
                avg_salary
            ) = cur.fetchone()


        conn.commit()


        # ====================================================
        # PRINT SUMMARY
        # ====================================================

        print(
            "\nCSV upload finished."
        )


        print(
            "\nR1p:"
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
            "\nR2p:"
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
            "  R1.eid = R2.eid matching rows =",
            eid_join_count
        )


        print(
            "\nDefaultSalary:"
        )

        print(
            "  min =",
            min_salary
        )

        print(
            "  max =",
            max_salary
        )

        print(
            "  avg =",
            avg_salary
        )


        print(
            "\nSelected R1 blocks:",
            len(
                r1_blocks
            )
        )

        print(
            "Selected R2 blocks:",
            len(
                r2_blocks
            )
        )


    except Exception:

        conn.rollback()

        raise


    finally:

        conn.close()


# ============================================================
# COMMAND-LINE ARGUMENTS
# ============================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Upload the R1 and R2 BID relations for the "
            "aggregate-query experiment."
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
# MAIN
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


    upload_relations(
        r1_csv=
            str(r1_path),

        r2_csv=
            str(r2_path),
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
