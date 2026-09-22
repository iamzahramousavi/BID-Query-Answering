import argparse
import csv
import re
import time

from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_values


# ============================================================
# FULL / GLOBAL MARKOVIEWS
#
# ProvSQL 1.12.0
#
# CSV paths are supplied at run time through --r1 and --r2.
# This keeps the public repository independent of local paths.
# ============================================================


def build_relations(r1_csv: str, r2_csv: str):

    return [
        {
            "query_name": "R1",
            "csv_path": r1_csv,
            "block_column": "block_id",
            "project_columns": [
                "eid",
                "Education",
                "Experience",
                "Position",
            ],
            "weight_column": "probability",
        },
        {
            "query_name": "R2",
            "csv_path": r2_csv,
            "block_column": "block_id",
            "project_columns": [
                "eid",
                "Position",
                "SalaryBand",
            ],
            "weight_column": "probability",
        },
    ]


# ============================================================
# DATABASE
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 55435,
    "dbname": "tutorial",
    "user": "test",
}


# ============================================================
# OPTIONS
# ============================================================

DEBUG = False

REBUILD_INPUTS_AND_PREPROCESS = True

SELECTED_BLOCK_IDS_PER_RELATION = None

MAX_BLOCKS_PER_RELATION = None

ROUND_CLOSE_TO_ONE_EPS = 1e-12


# ============================================================
# BOOLEAN EVENT
# ============================================================

BOOLEAN_EVENT_SQL = "SELECT TRUE AS k"


# ============================================================
# DEFAULT QUERY
#
# Join:
#   R1.eid = R2.eid
#
# Position='Senior':
#   selection condition on R1
# ============================================================

DEFAULT_USER_SQL = """SELECT TRUE AS k
FROM R1 r1
JOIN R2 r2
ON r1.eid = r2.eid
WHERE r1.Education = 'PhD'
AND r1.Experience = '0-15'
AND r1.Position = 'Senior'
AND r2.SalaryBand = '121k+';"""


# ============================================================
# CONNECTION
# ============================================================

def connect():
    return psycopg2.connect(**DB_CONFIG)


# ============================================================
# UTILITIES
# ============================================================

def normalize_header(name: str) -> str:
    return str(name).strip().lower()


def sanitize_token(value: Any) -> str:
    value = str(value).strip().replace(" ", "_")
    value = re.sub(
        r"[^A-Za-z0-9_]+",
        "",
        value
    )

    return value if value else "EMPTY"


def sql_quote(value: Any) -> str:
    s = str(value)

    return "'" + s.replace("'", "''") + "'"


def numericish_sort_key(value: Any):
    s = str(value).strip()

    if re.fullmatch(r"-?\d+", s):
        return 0, int(s)

    return 1, s


def make_tuple_id(
    relation_name: str,
    block_value: str,
    local_index: int
) -> str:

    return (
        f"{sanitize_token(relation_name)}"
        f"__{sanitize_token(block_value)}"
        f"__{local_index}"
    )


def format_probability(
    x: float,
    digits: int = 17
) -> str:

    s = f"{x:.{digits}f}"

    s = s.rstrip("0").rstrip(".")

    return s if s else "0"


# ============================================================
# BUILD MARKOVIEWS INPUT FROM CSV
# ============================================================

def build_relation_inputs_from_csv(
    relation_name: str,
    csv_path: str,
    block_column: str,
    project_columns: List[str],
    weight_column: str,
    max_blocks=None,
    selected_block_ids=None,
    round_close_to_one_eps: float = 1e-12,
):

    block_col = normalize_header(
        block_column
    )

    weight_col = normalize_header(
        weight_column
    )

    proj_cols = [
        normalize_header(c)
        for c in project_columns
    ]

    expected = {
        block_col,
        weight_col,
        *proj_cols,
    }

    selected_blocks_seen = set()

    selected_block_ids_set = (
        None
        if not selected_block_ids
        else {
            str(x)
            for x in selected_block_ids
        }
    )

    grouped_weights: Dict[
        Tuple[str, Tuple[str, ...]],
        float
    ] = defaultdict(float)


    # --------------------------------------------------------
    # Read CSV
    # --------------------------------------------------------

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

        normalized_fieldnames = {
            normalize_header(h)
            for h in reader.fieldnames
        }

        if not expected.issubset(
            normalized_fieldnames
        ):
            raise ValueError(
                "CSV is missing required columns.\n"
                f"CSV: {csv_path}\n"
                f"Expected at least: {sorted(expected)}\n"
                f"Found: {reader.fieldnames}"
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

            block_value = row[
                block_col
            ]


            if selected_block_ids is not None:

                if (
                    block_value
                    not in selected_block_ids_set
                ):
                    continue


            elif max_blocks is not None:

                if (
                    block_value
                    not in selected_blocks_seen
                ):

                    if (
                        len(selected_blocks_seen)
                        >= max_blocks
                    ):
                        continue

                    selected_blocks_seen.add(
                        block_value
                    )


            w = float(
                row[
                    weight_col
                ]
            )


            if w < 0.0:
                raise ValueError(
                    "BID weight must be non-negative; "
                    f"got {w} in block {block_value}"
                )


            projected_values = tuple(
                row[col]
                for col in proj_cols
            )


            grouped_weights[
                (
                    block_value,
                    projected_values
                )
            ] += w


    # --------------------------------------------------------
    # Projected rows
    # --------------------------------------------------------

    projected_rows: List[
        Dict[str, str]
    ] = []


    for (
        block_value,
        projected_values
    ), summed_w in grouped_weights.items():

        if (
            abs(
                1.0 - summed_w
            )
            <= round_close_to_one_eps
        ):
            summed_w = 1.0


        row = {
            "block_id":
                block_value,

            "weight":
                str(summed_w),
        }


        for i, col in enumerate(
            proj_cols
        ):
            row[col] = (
                projected_values[i]
            )


        projected_rows.append(
            row
        )


    projected_rows.sort(
        key=lambda r: (
            numericish_sort_key(
                r["block_id"]
            ),
            *(
                str(r[c])
                for c in proj_cols
            ),
        )
    )


    # --------------------------------------------------------
    # MarkoViews translation
    # --------------------------------------------------------

    prime_rows = []

    vboth_rows = []

    vnone_rows = []

    tuples_per_block: Dict[
        str,
        List[str]
    ] = defaultdict(list)

    seen_blocks = set()


    for row in projected_rows:

        block_value = row[
            "block_id"
        ]


        projected_values = [
            row[c]
            for c in proj_cols
        ]


        w = float(
            row[
                "weight"
            ]
        )


        # ----------------------------------------------------
        # p' = p / (1 + p)
        # ----------------------------------------------------

        p0 = (
            w
            /
            (
                1.0 + w
            )
        )


        local_index = (
            len(
                tuples_per_block[
                    block_value
                ]
            )
            + 1
        )


        tuple_id = make_tuple_id(
            relation_name,
            block_value,
            local_index
        )


        prime_rows.append(
            tuple(
                [
                    tuple_id,
                    block_value,
                ]
                + projected_values
                + [
                    p0
                ]
            )
        )


        tuples_per_block[
            block_value
        ].append(
            tuple_id
        )


        # ----------------------------------------------------
        # One Vnone helper per block
        # ----------------------------------------------------

        if (
            block_value
            not in seen_blocks
        ):

            vnone_rows.append(
                (
                    block_value,
                    1.0
                )
            )

            seen_blocks.add(
                block_value
            )


    # --------------------------------------------------------
    # Vboth pairs
    # --------------------------------------------------------

    for (
        block_value,
        tuple_ids
    ) in tuples_per_block.items():

        for t1, t2 in combinations(
            tuple_ids,
            2
        ):

            vboth_rows.append(
                (
                    block_value,
                    t1,
                    t2,
                    1.0
                )
            )


    return {
        "projected_rows":
            projected_rows,

        "ordinary_attributes":
            proj_cols,

        "prime_rows":
            prime_rows,

        "vboth_rows":
            vboth_rows,

        "vnone_rows":
            vnone_rows,
    }


# ============================================================
# UPLOAD INPUT TABLES
# ============================================================

def upload_relation_inputs(
    cur,
    rel_prefix: str,
    ordinary_attrs: List[str],
    prime_rows,
    vboth_rows,
    vnone_rows,
):

    # --------------------------------------------------------
    # Drop old input tables
    # --------------------------------------------------------

    cur.execute(
        f"""
        DROP TABLE IF EXISTS
        provsql_test.{rel_prefix}_prime_input
        CASCADE;
        """
    )


    cur.execute(
        f"""
        DROP TABLE IF EXISTS
        provsql_test.{rel_prefix}_vboth_input
        CASCADE;
        """
    )


    cur.execute(
        f"""
        DROP TABLE IF EXISTS
        provsql_test.{rel_prefix}_vnone_input
        CASCADE;
        """
    )


    ordinary_cols_sql = ",\n            ".join(
        f"{col} TEXT"
        for col in ordinary_attrs
    )


    # --------------------------------------------------------
    # Prime input
    # --------------------------------------------------------

    cur.execute(
        f"""
        CREATE TABLE
        provsql_test.{rel_prefix}_prime_input
        (
            tuple_id TEXT,
            block_id TEXT,
            {ordinary_cols_sql},
            p0 DOUBLE PRECISION
        );
        """
    )


    # --------------------------------------------------------
    # Vboth input
    # --------------------------------------------------------

    cur.execute(
        f"""
        CREATE TABLE
        provsql_test.{rel_prefix}_vboth_input
        (
            block_id TEXT,
            tuple_id_1 TEXT,
            tuple_id_2 TEXT,
            p0 DOUBLE PRECISION
        );
        """
    )


    # --------------------------------------------------------
    # Vnone input
    # --------------------------------------------------------

    cur.execute(
        f"""
        CREATE TABLE
        provsql_test.{rel_prefix}_vnone_input
        (
            block_id TEXT,
            p0 DOUBLE PRECISION
        );
        """
    )


    # --------------------------------------------------------
    # Insert prime
    # --------------------------------------------------------

    prime_columns = (
        [
            "tuple_id",
            "block_id",
        ]
        + ordinary_attrs
        + [
            "p0"
        ]
    )


    if prime_rows:

        execute_values(
            cur,
            f"""
            INSERT INTO
            provsql_test.{rel_prefix}_prime_input
            (
                {', '.join(prime_columns)}
            )
            VALUES %s
            """,
            prime_rows,
            template=(
                "("
                + ", ".join(
                    ["%s"]
                    * len(
                        prime_columns
                    )
                )
                + ")"
            ),
        )


    # --------------------------------------------------------
    # Insert Vboth
    # --------------------------------------------------------

    if vboth_rows:

        execute_values(
            cur,
            f"""
            INSERT INTO
            provsql_test.{rel_prefix}_vboth_input
            (
                block_id,
                tuple_id_1,
                tuple_id_2,
                p0
            )
            VALUES %s
            """,
            vboth_rows,
            template="(%s, %s, %s, %s)",
        )


    # --------------------------------------------------------
    # Insert Vnone
    # --------------------------------------------------------

    if vnone_rows:

        execute_values(
            cur,
            f"""
            INSERT INTO
            provsql_test.{rel_prefix}_vnone_input
            (
                block_id,
                p0
            )
            VALUES %s
            """,
            vnone_rows,
            template="(%s, %s)",
        )


# ============================================================
# PREPROCESS
# ============================================================

def run_preprocess(conn):

    with conn.cursor() as cur:

        cur.execute(
            """
            SELECT
            provsql_test.preprocess_r1();
            """
        )

        cur.execute(
            """
            SELECT
            provsql_test.preprocess_r2();
            """
        )

    conn.commit()


# ============================================================
# CHECK SQL SETUP
# ============================================================

def check_installed_sql(cur):

    cur.execute(
        """
        SELECT COUNT(*)

        FROM pg_proc p

        JOIN pg_namespace n
          ON n.oid = p.pronamespace

        WHERE n.nspname =
              'provsql_test'

          AND p.proname IN (
              'preprocess_r1',
              'preprocess_r2',
              'p1_e'
          );
        """
    )


    if (
        cur.fetchone()[0]
        < 3
    ):

        raise RuntimeError(
            "Run the SQL setup first. "
            "It must define preprocess_r1(), "
            "preprocess_r2(), and p1_E()."
        )


    # --------------------------------------------------------
    # truth_event must exist
    # --------------------------------------------------------

    cur.execute(
        """
        SELECT to_regclass(
            'provsql_test.truth_event'
        );
        """
    )


    truth_table = (
        cur.fetchone()[0]
    )


    if truth_table is None:

        raise RuntimeError(
            "Table provsql_test.truth_event "
            "does not exist. "
            "Run the SQL setup first."
        )


# ============================================================
# EVALUATE EVENT
# ============================================================

def p1_E(
    cur,
    event_sql: str
) -> float:

    cur.execute(
        """
        SELECT
        provsql_test.p1_E(%s);
        """,
        (
            event_sql,
        )
    )


    value = (
        cur.fetchone()[0]
    )


    return (
        0.0
        if value is None
        else float(
            value
        )
    )


# ============================================================
# SQL TEXT UTILITIES
# ============================================================

def normalize_sql_text(
    sql: str
) -> str:

    return (
        sql
        .replace(
            "\u2018",
            "'"
        )
        .replace(
            "\u2019",
            "'"
        )
        .replace(
            "\u201c",
            '"'
        )
        .replace(
            "\u201d",
            '"'
        )
    )


def strip_trailing_semicolon(
    sql: str
) -> str:

    return (
        normalize_sql_text(
            sql
        )
        .strip()
        .rstrip(";")
        .strip()
    )


def split_where_conditions(
    where_sql: str
) -> List[str]:

    parts = []

    buf = []

    depth = 0

    in_quote = False

    i = 0

    s = (
        where_sql.strip()
    )


    while i < len(s):

        ch = s[i]


        if ch == "'":

            in_quote = (
                not in_quote
            )

            buf.append(
                ch
            )

            i += 1

            continue


        if not in_quote:

            if ch == "(":
                depth += 1

            elif ch == ")":
                depth -= 1


            if depth == 0:

                # --------------------------------------------
                # Reject OR
                # --------------------------------------------

                if (
                    s[
                        i:i + 2
                    ].upper()
                    == "OR"
                ):

                    before = (
                        s[i - 1]
                        if i > 0
                        else " "
                    )

                    after = (
                        s[i + 2]
                        if (
                            i + 2
                            < len(s)
                        )
                        else " "
                    )


                    if (
                        before.isspace()
                        and
                        after.isspace()
                    ):

                        raise ValueError(
                            "OR conditions are "
                            "not supported."
                        )


                # --------------------------------------------
                # Split AND conditions
                # --------------------------------------------

                if (
                    s[
                        i:i + 3
                    ].upper()
                    == "AND"
                ):

                    before = (
                        s[i - 1]
                        if i > 0
                        else " "
                    )

                    after = (
                        s[i + 3]
                        if (
                            i + 3
                            < len(s)
                        )
                        else " "
                    )


                    if (
                        before.isspace()
                        and
                        after.isspace()
                    ):

                        parts.append(
                            "".join(
                                buf
                            ).strip()
                        )

                        buf = []

                        i += 3

                        continue


        buf.append(
            ch
        )

        i += 1


    if buf:

        parts.append(
            "".join(
                buf
            ).strip()
        )


    return [
        p
        for p in parts
        if p
    ]


# ============================================================
# SELECT CLAUSE PARSER
# ============================================================

def parse_select_clause(
    select_sql: str,
    alias_r1: str,
    alias_r2: str,
    relation_specs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:

    select_clean = (
        " ".join(
            select_sql
            .strip()
            .split()
        )
    )


    if re.fullmatch(
        r"TRUE\s+AS\s+k",
        select_clean,
        flags=re.IGNORECASE,
    ):

        return {
            "select_kind":
                "boolean",

            "projection_relation":
                None,

            "projection_attr":
                None,

            "distinct":
                False,
        }


    projection_match = (
        re.fullmatch(
            r"(DISTINCT\s+)?"
            r"([A-Za-z_][A-Za-z0-9_]*)\."
            r"([A-Za-z_][A-Za-z0-9_]*)",
            select_clean,
            flags=re.IGNORECASE,
        )
    )


    if not projection_match:

        raise ValueError(
            "Unsupported SELECT clause."
        )


    is_distinct = (
        projection_match.group(1)
        is not None
    )


    if not is_distinct:

        raise ValueError(
            "Projection queries must "
            "use SELECT DISTINCT."
        )


    alias = (
        projection_match.group(2)
    )


    attr = normalize_header(
        projection_match.group(3)
    )


    if (
        alias.lower()
        ==
        alias_r1.lower()
    ):

        relation_name = "R1"


    elif (
        alias.lower()
        ==
        alias_r2.lower()
    ):

        relation_name = "R2"


    else:

        raise ValueError(
            f"Unknown SELECT alias "
            f"'{alias}'."
        )


    valid_attrs = (
        [
            "block_id"
        ]
        +
        relation_specs[
            relation_name
        ][
            "ordinary_attributes"
        ]
    )


    if (
        attr
        not in valid_attrs
    ):

        raise ValueError(
            f"Unknown SELECT attribute "
            f"'{attr}' for "
            f"{relation_name}."
        )


    return {
        "select_kind":
            "projection",

        "projection_relation":
            relation_name,

        "projection_attr":
            attr,

        "distinct":
            True,
    }


# ============================================================
# USER QUERY PARSER
# ============================================================

def parse_sql_user_query(
    sql: str,
    relation_specs: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:

    s = (
        strip_trailing_semicolon(
            sql
        )
    )


    pattern = re.compile(
        r"""
        ^\s*SELECT\s+
        (?P<select>.+?)\s+
        FROM\s+R1\s+
        (?P<alias_r1>[A-Za-z_][A-Za-z0-9_]*)\s+
        JOIN\s+R2\s+
        (?P<alias_r2>[A-Za-z_][A-Za-z0-9_]*)\s+
        ON\s+
        (?P<on>.+?)\s+
        WHERE\s+
        (?P<where>.+)
        \s*$
        """,
        flags=(
            re.IGNORECASE
            |
            re.DOTALL
            |
            re.VERBOSE
        ),
    )


    m = pattern.match(
        s
    )


    if not m:

        raise ValueError(
            "Unsupported SQL format.\n"
            "Expected the R1/R2 join query."
        )


    select_sql = (
        m.group(
            "select"
        ).strip()
    )


    alias_r1 = (
        m.group(
            "alias_r1"
        )
    )


    alias_r2 = (
        m.group(
            "alias_r2"
        )
    )


    on_sql = (
        m.group(
            "on"
        ).strip()
    )


    where_sql = (
        m.group(
            "where"
        ).strip()
    )


    select_info = (
        parse_select_clause(
            select_sql=
                select_sql,

            alias_r1=
                alias_r1,

            alias_r2=
                alias_r2,

            relation_specs=
                relation_specs,
        )
    )


    # --------------------------------------------------------
    # Join parsing
    # --------------------------------------------------------

    on_match = (
        re.fullmatch(
            rf"{re.escape(alias_r1)}\."
            rf"([A-Za-z_][A-Za-z0-9_]*)"
            rf"\s*=\s*"
            rf"{re.escape(alias_r2)}\."
            rf"([A-Za-z_][A-Za-z0-9_]*)",
            on_sql,
            flags=re.IGNORECASE,
        )
    )


    if not on_match:

        raise ValueError(
            "Only one equality join "
            "is supported."
        )


    join_left = normalize_header(
        on_match.group(1)
    )


    join_right = normalize_header(
        on_match.group(2)
    )


    if (
        join_left != "eid"
        or
        join_right != "eid"
    ):

        raise ValueError(
            "The join must be:\n"
            "r1.eid = r2.eid"
        )


    conds_r1: List[
        Tuple[str, str]
    ] = []


    conds_r2: List[
        Tuple[str, str]
    ] = []


    for cond in (
        split_where_conditions(
            where_sql
        )
    ):

        m1 = re.fullmatch(
            rf"{re.escape(alias_r1)}\."
            rf"([A-Za-z_][A-Za-z0-9_]*)"
            rf"\s*=\s*'(.*)'",
            cond,
            flags=(
                re.IGNORECASE
                |
                re.DOTALL
            ),
        )


        m2 = re.fullmatch(
            rf"{re.escape(alias_r2)}\."
            rf"([A-Za-z_][A-Za-z0-9_]*)"
            rf"\s*=\s*'(.*)'",
            cond,
            flags=(
                re.IGNORECASE
                |
                re.DOTALL
            ),
        )


        if m1:

            attr = normalize_header(
                m1.group(1)
            )


            val = (
                m1.group(2)
                .replace(
                    "''",
                    "'"
                )
            )


            if attr == "block_id":

                raise ValueError(
                    "Do not use block_id "
                    "in the observed query."
                )


            if (
                attr
                not in
                relation_specs[
                    "R1"
                ][
                    "ordinary_attributes"
                ]
            ):

                raise ValueError(
                    f"Unknown R1 attribute "
                    f"'{attr}'."
                )


            conds_r1.append(
                (
                    attr,
                    val
                )
            )


            continue


        if m2:

            attr = normalize_header(
                m2.group(1)
            )


            val = (
                m2.group(2)
                .replace(
                    "''",
                    "'"
                )
            )


            if attr == "block_id":

                raise ValueError(
                    "Do not use block_id "
                    "in the observed query."
                )


            if (
                attr
                not in
                relation_specs[
                    "R2"
                ][
                    "ordinary_attributes"
                ]
            ):

                raise ValueError(
                    f"Unknown R2 attribute "
                    f"'{attr}'."
                )


            conds_r2.append(
                (
                    attr,
                    val
                )
            )


            continue


        raise ValueError(
            f"Unsupported WHERE condition: "
            f"{cond!r}"
        )


    return {
        "select_kind":
            select_info[
                "select_kind"
            ],

        "projection_relation":
            select_info[
                "projection_relation"
            ],

        "projection_attr":
            select_info[
                "projection_attr"
            ],

        "distinct":
            select_info[
                "distinct"
            ],

        "alias_r1":
            alias_r1,

        "alias_r2":
            alias_r2,

        "join_attr":
            "eid",

        "conds_r1":
            conds_r1,

        "conds_r2":
            conds_r2,

        "user_sql":
            s,
    }


# ============================================================
# CONDITION HELPERS
# ============================================================

def row_satisfies_conditions(
    row: Dict[str, str],
    conds: List[Tuple[str, str]]
) -> bool:

    for attr, val in conds:

        key = normalize_header(
            attr
        )


        if (
            key not in row
            or
            str(
                row[key]
            )
            !=
            str(
                val
            )
        ):

            return False


    return True


def conds_to_where_sql(
    conds: List[Tuple[str, str]],
    alias: str,
    valid_columns: List[str],
) -> str:

    parts = []


    for attr, val in conds:

        attr = normalize_header(
            attr
        )


        if (
            attr
            not in valid_columns
        ):

            raise ValueError(
                f"Unknown attribute "
                f"'{attr}'."
            )


        parts.append(
            f"{alias}.{attr} "
            f"= "
            f"{sql_quote(val)}"
        )


    return (
        " AND ".join(
            parts
        )
        if parts
        else "TRUE"
    )


# ============================================================
# RELATION SPECS
# ============================================================

def build_relation_specs() -> Dict[
    str,
    Dict[str, Any]
]:

    return {
        "R1": {
            "rel_prefix":
                "r1",

            "prime_table":
                "r1_prime",

            "vboth_table":
                "r1_vboth",

            "vnone_table":
                "r1_vnone",

            "ordinary_attributes": [
                "eid",
                "education",
                "experience",
                "position",
            ],
        },


        "R2": {
            "rel_prefix":
                "r2",

            "prime_table":
                "r2_prime",

            "vboth_table":
                "r2_vboth",

            "vnone_table":
                "r2_vnone",

            "ordinary_attributes": [
                "eid",
                "position",
                "salaryband",
            ],
        },
    }


# ============================================================
# COMPILE Q
# ============================================================

def compile_global_q_sql(
    parsed: Dict[str, Any],
    relation_specs: Dict[str, Dict[str, Any]],
    projection_filter: Optional[
        Dict[str, str]
    ] = None,
) -> str:

    r1_where = (
        conds_to_where_sql(
            parsed[
                "conds_r1"
            ],
            "r1",
            relation_specs[
                "R1"
            ][
                "ordinary_attributes"
            ],
        )
    )


    r2_where = (
        conds_to_where_sql(
            parsed[
                "conds_r2"
            ],
            "r2",
            relation_specs[
                "R2"
            ][
                "ordinary_attributes"
            ],
        )
    )


    parts = [
        BOOLEAN_EVENT_SQL,

        "FROM "
        "provsql_test.r1_prime r1",

        "JOIN "
        "provsql_test.r2_prime r2",

        "  ON "
        "r1.eid = r2.eid",

        "WHERE TRUE",
    ]


    if (
        r1_where
        != "TRUE"
    ):

        parts.append(
            f"  AND "
            f"{r1_where}"
        )


    if (
        r2_where
        != "TRUE"
    ):

        parts.append(
            f"  AND "
            f"{r2_where}"
        )


    if (
        projection_filter
        is not None
    ):

        rel = (
            projection_filter[
                "relation"
            ]
        )


        attr = (
            projection_filter[
                "attr"
            ]
        )


        value = (
            projection_filter[
                "value"
            ]
        )


        alias = (
            "r1"
            if rel == "R1"
            else "r2"
        )


        if (
            attr
            == "block_id"
        ):

            parts.append(
                f"  AND "
                f"{alias}.block_id "
                f"= "
                f"{sql_quote(value)}"
            )


        else:

            parts.append(
                f"  AND "
                f"{alias}.{attr} "
                f"= "
                f"{sql_quote(value)}"
            )


    return "\n".join(
        parts
    )


# ============================================================
# U_NONE
# ============================================================

def compile_u_none_relation_sql(
    relation_name: str,
    relation_specs: Dict[str, Dict[str, Any]],
) -> str:

    spec = (
        relation_specs[
            relation_name
        ]
    )


    alias_name = (
        f"{spec['rel_prefix'].upper()}"
        f"_unone"
    )


    return (
        f"{BOOLEAN_EVENT_SQL}\n"

        "FROM (\n"

        f"    SELECT "
        f"vn.block_id\n"

        f"    FROM "
        f"provsql_test."
        f"{spec['vnone_table']} "
        f"vn\n"

        "\n"

        "    EXCEPT\n"

        "\n"

        f"    SELECT "
        f"p.block_id\n"

        f"    FROM "
        f"provsql_test."
        f"{spec['prime_table']} "
        f"p\n"

        f"    GROUP BY "
        f"p.block_id\n"

        f") {alias_name}"
    )


# ============================================================
# U_BOTH
# ============================================================

def compile_u_both_relation_sql(
    relation_name: str,
    relation_specs: Dict[str, Dict[str, Any]],
) -> str:

    spec = (
        relation_specs[
            relation_name
        ]
    )


    alias_name = (
        f"{spec['rel_prefix'].upper()}"
        f"_uboth"
    )


    return (
        f"{BOOLEAN_EVENT_SQL}\n"

        "FROM (\n"

        f"    SELECT "
        f"vb.block_id\n"

        f"    FROM "
        f"provsql_test."
        f"{spec['vboth_table']} "
        f"vb\n"

        f"    JOIN "
        f"provsql_test."
        f"{spec['prime_table']} "
        f"t1\n"

        f"      ON "
        f"vb.tuple_id_1 "
        f"= "
        f"t1.tuple_id\n"

        f"    JOIN "
        f"provsql_test."
        f"{spec['prime_table']} "
        f"t2\n"

        f"      ON "
        f"vb.tuple_id_2 "
        f"= "
        f"t2.tuple_id\n"

        f") {alias_name}"
    )


# ============================================================
# GLOBAL U_NONE
# ============================================================

def compile_global_u_none_sql(
    relation_specs: Dict[str, Dict[str, Any]]
) -> str:

    return (
        compile_u_none_relation_sql(
            "R1",
            relation_specs
        )
        +
        "\nUNION ALL\n"
        +
        compile_u_none_relation_sql(
            "R2",
            relation_specs
        )
    )


# ============================================================
# GLOBAL U_BOTH
# ============================================================

def compile_global_u_both_sql(
    relation_specs: Dict[str, Dict[str, Any]]
) -> str:

    return (
        compile_u_both_relation_sql(
            "R1",
            relation_specs
        )
        +
        "\nUNION ALL\n"
        +
        compile_u_both_relation_sql(
            "R2",
            relation_specs
        )
    )


# ============================================================
# GLOBAL U
#
# NO RELEVANT-BLOCK RESTRICTION.
# ============================================================

def compile_global_u_sql(
    relation_specs: Dict[str, Dict[str, Any]]
) -> str:

    return (
        compile_global_u_none_sql(
            relation_specs
        )
        +
        "\nUNION ALL\n"
        +
        compile_global_u_both_sql(
            relation_specs
        )
    )


# ============================================================
# SQL INDENT
# ============================================================

def indent_sql(
    sql: str,
    spaces: int = 4
) -> str:

    prefix = (
        " "
        * spaces
    )


    return "\n".join(
        prefix + line
        for line in sql.splitlines()
    )


# ============================================================
# NOT U
#
# TRUE EXCEPT U
# ============================================================

def compile_global_not_u_sql(
    u_sql: str
) -> str:

    return (
        "SELECT t.k\n"

        "FROM "
        "provsql_test."
        "truth_event t\n"

        "EXCEPT\n"

        "SELECT u.k\n"

        "FROM (\n"

        f"{indent_sql(u_sql, 4)}\n"

        ") u"
    )


# ============================================================
# Q AND NOT U
# ============================================================

def compile_global_q_and_not_u_sql(
    q_sql: str,
    not_u_sql: str,
) -> str:

    return (
        "SELECT TRUE AS k\n"

        "FROM (\n"

        f"{indent_sql(q_sql, 4)}\n"

        ") q_event\n"

        "JOIN (\n"

        f"{indent_sql(not_u_sql, 4)}\n"

        ") valid_event\n"

        "  ON "
        "q_event.k "
        "= "
        "valid_event.k"
    )


# ============================================================
# DIRECT MARKOVIEWS REDUCTION
#
# P0(Q) =
#
#        P1(Q AND NOT U)
#       ----------------
#           P1(NOT U)
# ============================================================

def probability_for_event_sql(
    cur,
    q_sql: str,
    u_sql: str,
    p1_not_u: Optional[
        float
    ] = None,
) -> Dict[str, Any]:

    not_u_sql = (
        compile_global_not_u_sql(
            u_sql
        )
    )


    q_and_not_u_sql = (
        compile_global_q_and_not_u_sql(
            q_sql=
                q_sql,

            not_u_sql=
                not_u_sql,
        )
    )


    # --------------------------------------------------------
    # P1(NOT U)
    # --------------------------------------------------------

    if (
        p1_not_u
        is None
    ):

        p1_not_u = (
            p1_E(
                cur,
                not_u_sql
            )
        )


    # --------------------------------------------------------
    # P1(Q AND NOT U)
    # --------------------------------------------------------

    p1_q_and_not_u = (
        p1_E(
            cur,
            q_and_not_u_sql
        )
    )


    # ========================================================
    # IMPORTANT PRINTING FIX
    #
    # Use scientific notation.
    #
    # Tiny nonzero numbers will now appear as:
    #
    # 2.13456789012345678e-25
    #
    # instead of:
    #
    # 0
    # ========================================================

    print(
        "\n===== DIRECT "
        "MARKOVIEWS TERMS ====="
    )


    print(
        "P1(NOT U) = "
        f"{p1_not_u:.17e}"
    )


    print(
        "P1(Q AND NOT U) = "
        f"{p1_q_and_not_u:.17e}"
    )


    print(
        "==================================="
    )


    if (
        p1_not_u
        <= 0.0
    ):

        raise RuntimeError(
            "P1(NOT U) evaluated "
            "to exactly 0.0."
        )


    # --------------------------------------------------------
    # Conditional probability
    # --------------------------------------------------------

    p_q = (
        p1_q_and_not_u
        /
        p1_not_u
    )


    # --------------------------------------------------------
    # Tiny floating-point cleanup
    # --------------------------------------------------------

    if (
        p_q < 0.0
        and
        abs(p_q)
        < 1e-12
    ):

        p_q = 0.0


    if (
        p_q > 1.0
        and
        abs(
            p_q - 1.0
        )
        < 1e-12
    ):

        p_q = 1.0


    if (
        p_q < -1e-12
        or
        p_q > 1.0 + 1e-12
    ):

        raise RuntimeError(
            "Invalid probability.\n"
            f"P1(NOT U) = "
            f"{p1_not_u:.17e}\n"
            f"P1(Q AND NOT U) = "
            f"{p1_q_and_not_u:.17e}\n"
            f"P0(Q) = "
            f"{p_q:.17g}"
        )


    return {
        "p_q":
            p_q,

        "p1_not_u":
            p1_not_u,

        "p1_q_and_not_u":
            p1_q_and_not_u,

        "not_u_sql":
            not_u_sql,

        "q_and_not_u_sql":
            q_and_not_u_sql,

        "numerical_case":
            "direct_valid_world_conditioning",
    }


# ============================================================
# BOOLEAN SUMMARY
# ============================================================

def compute_boolean_probability_summary(
    cur,
    parsed: Dict[str, Any],
    relation_specs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:

    q_sql = (
        compile_global_q_sql(
            parsed,
            relation_specs
        )
    )


    u_none_sql = (
        compile_global_u_none_sql(
            relation_specs
        )
    )


    u_both_sql = (
        compile_global_u_both_sql(
            relation_specs
        )
    )


    u_sql = (
        u_none_sql
        +
        "\nUNION ALL\n"
        +
        u_both_sql
    )


    event_res = (
        probability_for_event_sql(
            cur=
                cur,

            q_sql=
                q_sql,

            u_sql=
                u_sql,

            p1_not_u=
                None,
        )
    )


    return {
        "p_q":
            event_res[
                "p_q"
            ],

        "p1_not_u":
            event_res[
                "p1_not_u"
            ],

        "p1_q_and_not_u":
            event_res[
                "p1_q_and_not_u"
            ],

        "q_sql":
            q_sql,

        "u_none_sql":
            u_none_sql,

        "u_both_sql":
            u_both_sql,

        "u_sql":
            u_sql,

        "not_u_sql":
            event_res[
                "not_u_sql"
            ],

        "q_and_not_u_sql":
            event_res[
                "q_and_not_u_sql"
            ],

        "numerical_case":
            event_res[
                "numerical_case"
            ],
    }


# ============================================================
# PROJECTION CANDIDATES
# ============================================================

def candidate_projection_values(
    parsed: Dict[str, Any],
    rows_r1: List[
        Dict[str, str]
    ],
    rows_r2: List[
        Dict[str, str]
    ],
) -> List[str]:

    if (
        parsed[
            "select_kind"
        ]
        !=
        "projection"
    ):

        return []


    projection_relation = (
        parsed[
            "projection_relation"
        ]
    )


    projection_attr = (
        parsed[
            "projection_attr"
        ]
    )


    values = set()


    for r1 in rows_r1:

        if not (
            row_satisfies_conditions(
                r1,
                parsed[
                    "conds_r1"
                ]
            )
        ):

            continue


        for r2 in rows_r2:

            if not (
                row_satisfies_conditions(
                    r2,
                    parsed[
                        "conds_r2"
                    ]
                )
            ):

                continue


            if (
                str(
                    r1.get(
                        "eid",
                        ""
                    )
                )
                !=
                str(
                    r2.get(
                        "eid",
                        ""
                    )
                )
            ):

                continue


            if (
                projection_relation
                == "R1"
            ):

                values.add(
                    str(
                        r1[
                            projection_attr
                        ]
                    )
                )


            else:

                values.add(
                    str(
                        r2[
                            projection_attr
                        ]
                    )
                )


    return sorted(
        values,
        key=
            numericish_sort_key
    )


# ============================================================
# PROJECTION PROBABILITIES
# ============================================================

def compute_projection_probability_table(
    cur,
    parsed: Dict[str, Any],
    rows_r1: List[
        Dict[str, str]
    ],
    rows_r2: List[
        Dict[str, str]
    ],
    relation_specs: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:

    projection_relation = (
        parsed[
            "projection_relation"
        ]
    )


    projection_attr = (
        parsed[
            "projection_attr"
        ]
    )


    answer_values = (
        candidate_projection_values(
            parsed=
                parsed,

            rows_r1=
                rows_r1,

            rows_r2=
                rows_r2,
        )
    )


    if not answer_values:

        return []


    u_sql = (
        compile_global_u_sql(
            relation_specs
        )
    )


    not_u_sql = (
        compile_global_not_u_sql(
            u_sql
        )
    )


    p1_not_u = (
        p1_E(
            cur,
            not_u_sql
        )
    )


    if (
        p1_not_u
        <= 0.0
    ):

        raise RuntimeError(
            "P1(NOT U) evaluated "
            "to zero."
        )


    output_rows = []


    for value in answer_values:

        projection_filter = {
            "relation":
                projection_relation,

            "attr":
                projection_attr,

            "value":
                value,
        }


        q_value_sql = (
            compile_global_q_sql(
                parsed=
                    parsed,

                relation_specs=
                    relation_specs,

                projection_filter=
                    projection_filter,
            )
        )


        event_res = (
            probability_for_event_sql(
                cur=
                    cur,

                q_sql=
                    q_value_sql,

                u_sql=
                    u_sql,

                p1_not_u=
                    p1_not_u,
            )
        )


        output_rows.append(
            {
                projection_attr:
                    value,

                "probability":
                    event_res[
                        "p_q"
                    ],

                "q_sql":
                    q_value_sql,

                "numerical_case":
                    event_res[
                        "numerical_case"
                    ],
            }
        )


    return output_rows


# ============================================================
# BOOLEAN WRAPPER
# ============================================================

def p_Q(
    cur,
    parsed: Dict[str, Any],
    rows_r1: List[
        Dict[str, str]
    ],
    rows_r2: List[
        Dict[str, str]
    ],
    relation_specs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:

    del rows_r1
    del rows_r2


    return (
        compute_boolean_probability_summary(
            cur=
                cur,

            parsed=
                parsed,

            relation_specs=
                relation_specs,
        )
    )


# ============================================================
# PRINT BOOLEAN RESULT
#
# PRINTING FIX IS ALSO HERE.
# ============================================================

def print_boolean_result(
    summary: Dict[str, Any],
    preprocess_time: float
):

    print(
        "\n===== RESULT ====="
    )


    # --------------------------------------------------------
    # Scientific notation for tiny probabilities
    # --------------------------------------------------------

    print(
        "P1(NOT U) = "
        f"{summary['p1_not_u']:.17e}"
    )


    print(
        "P1(Q AND NOT U) = "
        f"{summary['p1_q_and_not_u']:.17e}"
    )


    # --------------------------------------------------------
    # Normal precision for final P(Q)
    # --------------------------------------------------------

    print(
        "P(Q) = "
        f"{summary['p_q']:.17g}"
    )


    print(
        "Preprocess time = "
        f"{round(preprocess_time, 4)} sec"
    )


    print(
        "=================="
    )


# ============================================================
# PRINT PROJECTION RESULT
# ============================================================

def print_projection_result(
    parsed: Dict[str, Any],
    rows: List[
        Dict[str, Any]
    ],
    preprocess_time: float,
):

    label = (
        parsed[
            "projection_attr"
        ]
    )


    print(
        "\n===== RESULT ====="
    )


    print(
        f"{label} | P(Q)"
    )


    print(
        "------------------"
    )


    for row in rows:

        print(
            f"{row[label]}"
            f" | "
            f"{row['probability']:.17g}"
        )


    print(
        "Preprocess time = "
        f"{round(preprocess_time, 4)} sec"
    )


    print(
        "=================="
    )


# ============================================================
# COMMAND-LINE ARGUMENTS
# ============================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Full/global MarkoViews evaluation over BID "
            "relations using ProvSQL."
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

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print generated Q, U, NOT U, and Q AND NOT U SQL.",
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

    relations = build_relations(
        r1_csv=str(r1_path),
        r2_csv=str(r2_path),
    )

    relation_specs = (
        build_relation_specs()
    )


    relation_inputs = {}


    rows_by_relation: Dict[
        str,
        List[
            Dict[str, str]
        ]
    ] = {}


    # --------------------------------------------------------
    # Read and translate R1/R2
    # --------------------------------------------------------

    for rel_cfg in relations:

        rel_name = (
            rel_cfg[
                "query_name"
            ]
            .strip()
            .upper()
        )


        print(
            f"\nReading and translating "
            f"{rel_name} from: "
            f"{rel_cfg['csv_path']}"
        )


        built = (
            build_relation_inputs_from_csv(
                relation_name=
                    rel_name,

                csv_path=
                    rel_cfg[
                        "csv_path"
                    ],

                block_column=
                    rel_cfg[
                        "block_column"
                    ],

                project_columns=
                    rel_cfg[
                        "project_columns"
                    ],

                weight_column=
                    rel_cfg[
                        "weight_column"
                    ],

                max_blocks=
                    MAX_BLOCKS_PER_RELATION,

                selected_block_ids=
                    SELECTED_BLOCK_IDS_PER_RELATION,

                round_close_to_one_eps=
                    ROUND_CLOSE_TO_ONE_EPS,
            )
        )


        relation_inputs[
            rel_name
        ] = built


        rows_by_relation[
            rel_name
        ] = (
            built[
                "projected_rows"
            ]
        )


    # --------------------------------------------------------
    # Connect
    # --------------------------------------------------------

    conn = connect()


    try:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    current_database(),
                    current_user,
                    (
                        SELECT extversion
                        FROM pg_extension
                        WHERE extname =
                              'provsql'
                    );
                """
            )


            connected_info = (
                cur.fetchone()
            )


            print(
                "Connected as:",
                connected_info
            )


            if (
                connected_info[0]
                != "tutorial"
            ):

                raise RuntimeError(
                    "Wrong database. "
                    "Expected tutorial."
                )


            if (
                connected_info[2]
                != "1.12.0"
            ):

                raise RuntimeError(
                    "Wrong ProvSQL version. "
                    "Expected 1.12.0."
                )


            check_installed_sql(
                cur
            )


        preprocess_time = 0.0


        # ----------------------------------------------------
        # Upload and preprocess
        # ----------------------------------------------------

        if (
            REBUILD_INPUTS_AND_PREPROCESS
        ):

            with conn.cursor() as cur:

                cur.execute(
                    """
                    SET search_path TO
                    provsql_test,
                    provsql,
                    public;
                    """
                )


                cur.execute(
                    """
                    CREATE SCHEMA
                    IF NOT EXISTS
                    provsql_test;
                    """
                )


            conn.commit()


            for (
                rel_name,
                built
            ) in relation_inputs.items():

                spec = (
                    relation_specs[
                        rel_name
                    ]
                )


                with conn.cursor() as cur:

                    upload_relation_inputs(
                        cur=
                            cur,

                        rel_prefix=
                            spec[
                                "rel_prefix"
                            ],

                        ordinary_attrs=
                            spec[
                                "ordinary_attributes"
                            ],

                        prime_rows=
                            built[
                                "prime_rows"
                            ],

                        vboth_rows=
                            built[
                                "vboth_rows"
                            ],

                        vnone_rows=
                            built[
                                "vnone_rows"
                            ],
                    )


                conn.commit()


                print(
                    f"\n{rel_name}:"
                )


                print(
                    "  prime rows = "
                    f"{len(built['prime_rows'])}"
                )


                print(
                    "  vboth rows = "
                    f"{len(built['vboth_rows'])}"
                )


                print(
                    "  vnone rows = "
                    f"{len(built['vnone_rows'])}"
                )


            print(
                "\nRunning SQL "
                "preprocess functions..."
            )


            t0 = time.time()


            run_preprocess(
                conn
            )


            preprocess_time = (
                time.time()
                -
                t0
            )


            print(
                "Preprocess finished "
                f"in "
                f"{round(preprocess_time, 4)} "
                f"sec"
            )


        # ----------------------------------------------------
        # Query input
        # ----------------------------------------------------

        print(
            "\nEnter the SQL query."
        )


        print(
            "If you press Enter without "
            "typing a query, the default "
            "query below is used:"
        )


        print(
            DEFAULT_USER_SQL
        )


        lines = []


        while True:

            line = input()


            if not (
                line.strip()
            ):

                break


            lines.append(
                line.rstrip()
            )


        user_sql = (
            "\n".join(
                lines
            )
            .strip()
        )


        if not user_sql:

            user_sql = (
                DEFAULT_USER_SQL
            )


            print(
                "Using the default "
                "Boolean join query."
            )


        parsed = (
            parse_sql_user_query(
                user_sql,
                relation_specs
            )
        )


        print(
            "\nUser SQL:"
        )


        print(
            parsed[
                "user_sql"
            ]
            +
            ";"
        )


        print(
            "\ncomputing...",
            flush=True
        )


        # ----------------------------------------------------
        # Evaluate
        # ----------------------------------------------------

        with conn.cursor() as cur:

            if (
                parsed[
                    "select_kind"
                ]
                ==
                "boolean"
            ):

                try:

                    summary = p_Q(
                        cur=
                            cur,

                        parsed=
                            parsed,

                        rows_r1=
                            rows_by_relation[
                                "R1"
                            ],

                        rows_r2=
                            rows_by_relation[
                                "R2"
                            ],

                        relation_specs=
                            relation_specs,
                    )


                # =================================================
                # CLEAN HANDLING OF COMPILER STATUS 137
                #
                # Both d4 and dsharp can terminate with status 137
                # when the global formula exceeds available system
                # resources. This does not alter the computation; it
                # only reports the scalability failure cleanly.
                # =================================================

                except psycopg2.Error as exc:

                    error_text = str(exc)

                    status_137 = re.search(
                        r"(d4|dsharp) exited with status 137",
                        error_text,
                        flags=re.IGNORECASE,
                    )

                    if status_137:

                        conn.rollback()

                        compiler = status_137.group(1)

                        print(
                            "\n===== RESULT ====="
                        )

                        print(
                            "Knowledge compilation did not complete."
                        )

                        print(
                            f"ProvSQL/{compiler} exited with status 137."
                        )

                        print(
                            "P(Q) = NOT COMPUTED"
                        )

                        print(
                            "The run is recorded as a scalability/"
                            "resource-limit failure for the full/global "
                            "MarkoViews approach."
                        )

                        print(
                            "=================="
                        )

                        return

                    raise


                if DEBUG or args.debug:

                    print(
                        "\n===== GLOBAL "
                        "MARKOVIEWS EVENT SQL ====="
                    )


                    print(
                        "\nQ SQL:"
                    )


                    print(
                        summary[
                            "q_sql"
                        ]
                    )


                    print(
                        "\nU SQL:"
                    )


                    print(
                        summary[
                            "u_sql"
                        ]
                    )


                    print(
                        "\nNOT U SQL:"
                    )


                    print(
                        summary[
                            "not_u_sql"
                        ]
                    )


                    print(
                        "\nQ AND NOT U SQL:"
                    )


                    print(
                        summary[
                            "q_and_not_u_sql"
                        ]
                    )


                    print(
                        "\nNumerical case:"
                    )


                    print(
                        summary[
                            "numerical_case"
                        ]
                    )


                print_boolean_result(
                    summary,
                    preprocess_time
                )


            elif (
                parsed[
                    "select_kind"
                ]
                ==
                "projection"
            ):

                rows = (
                    compute_projection_probability_table(
                        cur=
                            cur,

                        parsed=
                            parsed,

                        rows_r1=
                            rows_by_relation[
                                "R1"
                            ],

                        rows_r2=
                            rows_by_relation[
                                "R2"
                            ],

                        relation_specs=
                            relation_specs,
                    )
                )


                print_projection_result(
                    parsed,
                    rows,
                    preprocess_time
                )


            else:

                raise RuntimeError(
                    "Unknown select_kind: "
                    f"{parsed['select_kind']}"
                )


    finally:

        conn.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
