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
# Query-Dependent Restricted MarkoViews
#
# ProvSQL 1.12.0
#
# This implementation evaluates the Boolean conjunctive query:
#
#   SELECT TRUE AS k
#   FROM R1 r1
#   JOIN R2 r2
#     ON r1.eid = r2.eid
#   WHERE r1.Education = 'PhD'
#     AND r1.Experience = '0-15'
#     AND r1.Position = 'Senior'
#     AND r2.SalaryBand = '121k+';
#
# The CSV paths are supplied on the command line so that the
# implementation does not depend on machine-specific paths.
#
# The knowledge compiler used by ProvSQL is configured in the
# accompanying SQL file through provsql_test.p1_E(...).
# ============================================================


# ============================================================
# DATABASE CONFIGURATION
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 55435,
    "dbname": "tutorial",
    "user": "test",
}


# ============================================================
# SETTINGS
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
# DEFAULT BOOLEAN QUERY
#
# Join:
#
#     r1.eid = r2.eid
#
# Position is a selection condition, not the join attribute.
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

    return format(
        float(x),
        f".{digits}g"
    )


def clamp_probability(
    x: float,
    eps: float = 1e-12
) -> float:

    if x < 0.0 and abs(x) <= eps:
        return 0.0

    if x > 1.0 and abs(x - 1.0) <= eps:
        return 1.0

    return x


# ============================================================
# BUILD MARKOVIEWS INPUT FROM ONE BID CSV
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
            abs(1.0 - summed_w)
            <= round_close_to_one_eps
        ):
            summed_w = 1.0

        row = {
            "block_id": block_value,
            "weight": str(summed_w),
        }

        for i, col in enumerate(
            proj_cols
        ):
            row[col] = projected_values[i]

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

        # MarkoViews transformation:
        #
        #     p' = p / (1 + p)

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
                +
                projected_values
                +
                [
                    p0
                ]
            )
        )

        tuples_per_block[
            block_value
        ].append(
            tuple_id
        )

        # One Vnone tuple per block.

        if block_value not in seen_blocks:

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
    # Vboth tuples:
    # one tuple for every pair of tuples in a block.
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
# UPLOAD GENERATED INPUT TABLES
# ============================================================

def upload_relation_inputs(
    cur,
    rel_prefix: str,
    ordinary_attrs: List[str],
    prime_rows,
    vboth_rows,
    vnone_rows,
):

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

    prime_columns = (
        [
            "tuple_id",
            "block_id",
        ]
        +
        ordinary_attrs
        +
        [
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
                +
                ", ".join(
                    ["%s"]
                    *
                    len(prime_columns)
                )
                +
                ")"
            ),
        )

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
            SELECT provsql_test.preprocess_r1();
            """
        )

        cur.execute(
            """
            SELECT provsql_test.preprocess_r2();
            """
        )

    conn.commit()


# ============================================================
# VERIFY SQL INSTALLATION
# ============================================================

def check_installed_sql(cur):

    cur.execute(
        """
        SELECT COUNT(*)
        FROM pg_proc p
        JOIN pg_namespace n
          ON n.oid = p.pronamespace
        WHERE n.nspname = 'provsql_test'
          AND p.proname IN (
              'preprocess_r1',
              'preprocess_r2',
              'p1_e'
          );
        """
    )

    if cur.fetchone()[0] < 3:

        raise RuntimeError(
            "Run the accompanying SQL script first. "
            "It must define preprocess_r1(), "
            "preprocess_r2(), and p1_E()."
        )


# ============================================================
# PROVSQL EVENT PROBABILITY
# ============================================================

def p1_E(
    cur,
    event_sql: str
) -> float:

    cur.execute(
        """
        SELECT provsql_test.p1_E(%s);
        """,
        (
            event_sql,
        )
    )

    value = cur.fetchone()[0]

    return (
        0.0
        if value is None
        else float(value)
    )


# ============================================================
# SQL QUERY PARSING
# ============================================================

def normalize_sql_text(
    sql: str
) -> str:

    return (
        sql
        .replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
    )


def strip_trailing_semicolon(
    sql: str
) -> str:

    return (
        normalize_sql_text(sql)
        .strip()
        .rstrip(";")
        .strip()
    )


def split_where_conditions(
    where_sql: str
) -> List[str]:

    parts = []
    buf = []

    in_quote = False

    i = 0

    s = where_sql.strip()

    while i < len(s):

        ch = s[i]

        if ch == "'":

            in_quote = not in_quote

            buf.append(ch)

            i += 1

            continue

        if not in_quote:

            # OR is not supported.

            if (
                s[i:i + 2].upper()
                == "OR"
            ):

                before = (
                    s[i - 1]
                    if i > 0
                    else " "
                )

                after = (
                    s[i + 2]
                    if i + 2 < len(s)
                    else " "
                )

                if (
                    before.isspace()
                    and
                    after.isspace()
                ):

                    raise ValueError(
                        "OR conditions are not supported."
                    )

            # Split on top-level AND.

            if (
                s[i:i + 3].upper()
                == "AND"
            ):

                before = (
                    s[i - 1]
                    if i > 0
                    else " "
                )

                after = (
                    s[i + 3]
                    if i + 3 < len(s)
                    else " "
                )

                if (
                    before.isspace()
                    and
                    after.isspace()
                ):

                    parts.append(
                        "".join(buf).strip()
                    )

                    buf = []

                    i += 3

                    continue

        buf.append(ch)

        i += 1

    if buf:

        parts.append(
            "".join(buf).strip()
        )

    return [
        p
        for p in parts
        if p
    ]


def parse_sql_user_query(
    sql: str,
    relation_specs: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:

    s = strip_trailing_semicolon(
        sql
    )

    pattern = re.compile(
        r"""
        ^\s*SELECT\s+TRUE\s+AS\s+k\s+
        FROM\s+R1\s+(?P<alias_r1>[A-Za-z_][A-Za-z0-9_]*)\s+
        JOIN\s+R2\s+(?P<alias_r2>[A-Za-z_][A-Za-z0-9_]*)\s+
        ON\s+(?P<on>.+?)\s+
        WHERE\s+(?P<where>.+)
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

    m = pattern.match(s)

    if not m:

        raise ValueError(
            "Unsupported SQL format.\n\n"
            "Expected a Boolean query of the form:\n\n"
            "SELECT TRUE AS k\n"
            "FROM R1 r1\n"
            "JOIN R2 r2\n"
            "ON r1.eid = r2.eid\n"
            "WHERE ...;"
        )

    alias_r1 = m.group(
        "alias_r1"
    )

    alias_r2 = m.group(
        "alias_r2"
    )

    on_sql = m.group(
        "on"
    ).strip()

    where_sql = m.group(
        "where"
    ).strip()

    # --------------------------------------------------------
    # Join
    # --------------------------------------------------------

    on_match = re.fullmatch(
        rf"{re.escape(alias_r1)}\."
        rf"([A-Za-z_][A-Za-z0-9_]*)"
        rf"\s*=\s*"
        rf"{re.escape(alias_r2)}\."
        rf"([A-Za-z_][A-Za-z0-9_]*)",
        on_sql,
        flags=re.IGNORECASE,
    )

    if not on_match:

        raise ValueError(
            "Only one equality join is supported."
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
            "The query must join R1 and R2 on eid:\n"
            "r1.eid = r2.eid"
        )

    conds_r1: List[
        Tuple[str, str]
    ] = []

    conds_r2: List[
        Tuple[str, str]
    ] = []

    # --------------------------------------------------------
    # WHERE conditions
    # --------------------------------------------------------

    for cond in split_where_conditions(
        where_sql
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
                    f"Unknown R1 attribute: {attr}"
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
                    f"Unknown R2 attribute: {attr}"
                )

            conds_r2.append(
                (
                    attr,
                    val
                )
            )

            continue

        raise ValueError(
            f"Unsupported WHERE condition: {cond}"
        )

    return {
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

        if (
            str(
                row.get(
                    normalize_header(attr),
                    ""
                )
            )
            !=
            str(val)
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

        if attr not in valid_columns:

            raise ValueError(
                f"Unknown attribute '{attr}'."
            )

        parts.append(
            f"{alias}.{attr} = {sql_quote(val)}"
        )

    return (
        " AND ".join(parts)
        if parts
        else "TRUE"
    )


# ============================================================
# RELATION SPECIFICATIONS
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
# BLOCK-LEVEL QUERY EVENT Q
# ============================================================

def compile_q(
    relation_name: str,
    block_id: str,
    join_attr: str,
    join_value: str,
    conds: List[Tuple[str, str]],
    relation_specs: Dict[str, Dict[str, Any]],
) -> str:

    spec = relation_specs[
        relation_name
    ]

    where = conds_to_where_sql(
        conds,
        "p",
        spec[
            "ordinary_attributes"
        ],
    )

    parts = [
        BOOLEAN_EVENT_SQL,

        f"FROM "
        f"provsql_test."
        f"{spec['prime_table']} p",

        f"WHERE "
        f"p.block_id = "
        f"{sql_quote(block_id)}",

        f"  AND "
        f"p.{join_attr} = "
        f"{sql_quote(join_value)}",
    ]

    if where != "TRUE":

        parts.append(
            f"  AND {where}"
        )

    return "\n".join(
        parts
    )


# ============================================================
# BLOCK-LEVEL VIOLATION EVENT U
# ============================================================

def compile_u(
    relation_name: str,
    block_id: str,
    relation_specs: Dict[str, Dict[str, Any]],
) -> str:

    spec = relation_specs[
        relation_name
    ]

    unone_alias = (
        f"{spec['rel_prefix'].upper()}_unone"
    )

    u_none = (
        f"{BOOLEAN_EVENT_SQL}\n"
        "FROM (\n"
        f"    SELECT vn.block_id\n"
        f"    FROM "
        f"provsql_test.{spec['vnone_table']} vn\n"
        f"    WHERE "
        f"vn.block_id = {sql_quote(block_id)}\n"
        "\n"
        "    EXCEPT\n"
        "\n"
        f"    SELECT p.block_id\n"
        f"    FROM "
        f"provsql_test.{spec['prime_table']} p\n"
        f"    WHERE "
        f"p.block_id = {sql_quote(block_id)}\n"
        f"    GROUP BY p.block_id\n"
        f") {unone_alias}"
    )

    uboth_alias = (
        f"{spec['rel_prefix'].upper()}_uboth"
    )

    u_both = (
        f"{BOOLEAN_EVENT_SQL}\n"
        "FROM (\n"
        f"    SELECT vb.block_id\n"
        f"    FROM "
        f"provsql_test.{spec['vboth_table']} vb\n"
        f"    JOIN "
        f"provsql_test.{spec['prime_table']} t1\n"
        f"      ON "
        f"vb.tuple_id_1 = t1.tuple_id\n"
        f"    JOIN "
        f"provsql_test.{spec['prime_table']} t2\n"
        f"      ON "
        f"vb.tuple_id_2 = t2.tuple_id\n"
        f"    WHERE "
        f"vb.block_id = {sql_quote(block_id)}\n"
        f") {uboth_alias}"
    )

    return (
        u_none
        +
        "\nUNION ALL\n"
        +
        u_both
    )


# ============================================================
# BLOCK-LEVEL MARKOVIEWS REDUCTION
#
#       P0(Q)
#       =
#       [P1(Q OR U) - P1(U)]
#       --------------------
#             1 - P1(U)
#
# The formula is applied only to the small block-level
# events used by the query-dependent computation.
# ============================================================

def exact_probability_for_event_sql(
    cur,
    q_sql: str,
    u_sql: str,
) -> Dict[str, Any]:

    q_or_u_sql = (
        q_sql
        +
        "\nUNION ALL\n"
        +
        u_sql
    )

    p1_q = p1_E(
        cur,
        q_sql
    )

    p1_u = p1_E(
        cur,
        u_sql
    )

    p1_q_or_u = p1_E(
        cur,
        q_or_u_sql
    )

    denom = (
        1.0
        -
        p1_u
    )

    if abs(denom) < 1e-15:

        raise RuntimeError(
            "Numerical cancellation in a "
            "block-level MarkoViews event."
        )

    p_exact = (
        (
            p1_q_or_u
            -
            p1_u
        )
        /
        denom
    )

    p_exact = clamp_probability(
        p_exact
    )

    return {
        "p1_q":
            p1_q,

        "p1_u":
            p1_u,

        "p1_q_or_u":
            p1_q_or_u,

        "p_exact":
            p_exact,
    }


# ============================================================
# BID-CORRECTED PROBABILITY FOR ONE BLOCK / eid
# ============================================================

def exact_mv_probability_for_single_relation_block_value(
    cur,
    relation_name: str,
    block_id: str,
    join_attr: str,
    join_value: str,
    conds: List[Tuple[str, str]],
    relation_specs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:

    q_sql = compile_q(
        relation_name=
            relation_name,

        block_id=
            block_id,

        join_attr=
            join_attr,

        join_value=
            join_value,

        conds=
            conds,

        relation_specs=
            relation_specs,
    )

    u_sql = compile_u(
        relation_name=
            relation_name,

        block_id=
            block_id,

        relation_specs=
            relation_specs,
    )

    event_res = exact_probability_for_event_sql(
        cur,
        q_sql,
        u_sql
    )

    return {
        "relation":
            relation_name,

        "block_id":
            block_id,

        "join_value":
            join_value,

        "p1_q":
            event_res[
                "p1_q"
            ],

        "p1_u":
            event_res[
                "p1_u"
            ],

        "p1_q_or_u":
            event_res[
                "p1_q_or_u"
            ],

        "p_bid":
            event_res[
                "p_exact"
            ],
    }


# ============================================================
# IDENTIFY QUERY-RELEVANT BLOCKS BY eid
# ============================================================

def matching_blocks_by_join_value(
    rows: List[Dict[str, str]],
    conds: List[Tuple[str, str]],
    join_attr: str,
) -> Dict[str, List[str]]:

    out: Dict[
        str,
        set
    ] = defaultdict(set)

    for row in rows:

        if not row_satisfies_conditions(
            row,
            conds
        ):
            continue

        join_value = str(
            row[
                join_attr
            ]
        )

        out[
            join_value
        ].add(
            str(
                row[
                    "block_id"
                ]
            )
        )

    return {
        value:
            sorted(
                list(blocks),
                key=
                    numericish_sort_key
            )

        for value, blocks
        in out.items()
    }


# ============================================================
# BID-CORRECTED DISTRIBUTION FOR ONE RELATION
#
# This is the query-dependent part of the evaluation.
#
# Only eids that can contribute to the Boolean query are
# retained.
# ============================================================

def compute_relation_join_value_distribution(
    cur,
    relation_name: str,
    rows: List[Dict[str, str]],
    conds: List[Tuple[str, str]],
    join_attr: str,
    join_values: List[str],
    relation_specs: Dict[str, Dict[str, Any]],
) -> Tuple[
    Dict[int, float],
    List[Dict[str, Any]]
]:

    value_to_bit = {
        z:
            1 << i

        for i, z
        in enumerate(
            join_values
        )
    }

    block_to_values: Dict[
        str,
        set
    ] = defaultdict(set)

    for row in rows:

        z = str(
            row.get(
                join_attr,
                ""
            )
        )

        if z not in value_to_bit:
            continue

        if not row_satisfies_conditions(
            row,
            conds
        ):
            continue

        block_to_values[
            str(
                row[
                    "block_id"
                ]
            )
        ].add(
            z
        )

    dist: Dict[
        int,
        float
    ] = {
        0:
            1.0
    }

    evaluations: List[
        Dict[str, Any]
    ] = []

    for block_id in sorted(
        block_to_values.keys(),
        key=
            numericish_sort_key
    ):

        values = sorted(
            block_to_values[
                block_id
            ],
            key=lambda x:
                join_values.index(x)
        )

        value_probs: Dict[
            str,
            float
        ] = {}

        for z in values:

            res = (
                exact_mv_probability_for_single_relation_block_value(
                    cur=
                        cur,

                    relation_name=
                        relation_name,

                    block_id=
                        block_id,

                    join_attr=
                        join_attr,

                    join_value=
                        z,

                    conds=
                        conds,

                    relation_specs=
                        relation_specs,
                )
            )

            value_probs[
                z
            ] = res[
                "p_bid"
            ]

            evaluations.append(
                res
            )

        p_any_relevant = sum(
            value_probs.values()
        )

        if (
            p_any_relevant > 1.0
            and
            abs(
                p_any_relevant
                -
                1.0
            )
            < 1e-10
        ):
            p_any_relevant = 1.0

        if (
            p_any_relevant
            >
            1.0
            +
            1e-8
        ):

            raise RuntimeError(
                f"Invalid probability distribution "
                f"for {relation_name} "
                f"block {block_id}: "
                f"{p_any_relevant}"
            )

        block_outcomes = [
            (
                0,
                max(
                    0.0,
                    1.0
                    -
                    p_any_relevant
                )
            )
        ]

        for z, p in value_probs.items():

            block_outcomes.append(
                (
                    value_to_bit[
                        z
                    ],
                    p
                )
            )

        new_dist: Dict[
            int,
            float
        ] = defaultdict(float)

        for old_mask, old_p in dist.items():

            for add_mask, add_p in block_outcomes:

                new_dist[
                    old_mask
                    |
                    add_mask
                ] += (
                    old_p
                    *
                    add_p
                )

        dist = dict(
            new_dist
        )

    total = sum(
        dist.values()
    )

    if total > 0.0:

        for key in list(
            dist.keys()
        ):

            dist[key] /= total

    return (
        dist,
        evaluations
    )


# ============================================================
# TID DISTRIBUTION FOR P1(Q)
#
# Event probabilities themselves are evaluated by ProvSQL.
# Python combines independent block-level events.
# ============================================================

def compute_relation_join_value_distribution_p0_provsql(
    cur,
    relation_name: str,
    rows: List[Dict[str, str]],
    conds: List[Tuple[str, str]],
    join_attr: str,
    join_values: List[str],
    relation_specs: Dict[str, Dict[str, Any]],
) -> Tuple[
    Dict[int, float],
    List[Dict[str, Any]]
]:

    value_to_bit = {
        z:
            1 << i

        for i, z
        in enumerate(
            join_values
        )
    }

    block_to_values: Dict[
        str,
        set
    ] = defaultdict(set)

    for row in rows:

        z = str(
            row.get(
                join_attr,
                ""
            )
        )

        if z not in value_to_bit:
            continue

        if not row_satisfies_conditions(
            row,
            conds
        ):
            continue

        block_to_values[
            str(
                row[
                    "block_id"
                ]
            )
        ].add(
            z
        )

    dist: Dict[
        int,
        float
    ] = {
        0:
            1.0
    }

    evaluations: List[
        Dict[str, Any]
    ] = []

    for block_id in sorted(
        block_to_values.keys(),
        key=
            numericish_sort_key
    ):

        value_event_probs: Dict[
            str,
            float
        ] = {}

        values = sorted(
            block_to_values[
                block_id
            ],
            key=lambda x:
                join_values.index(x)
        )

        for z in values:

            q_sql = compile_q(
                relation_name=
                    relation_name,

                block_id=
                    block_id,

                join_attr=
                    join_attr,

                join_value=
                    z,

                conds=
                    conds,

                relation_specs=
                    relation_specs,
            )

            p1_q = clamp_probability(
                p1_E(
                    cur,
                    q_sql
                )
            )

            value_event_probs[
                z
            ] = p1_q

            evaluations.append(
                {
                    "relation":
                        relation_name,

                    "block_id":
                        block_id,

                    "join_value":
                        z,

                    "p1_q":
                        p1_q,
                }
            )

        block_dist: Dict[
            int,
            float
        ] = {
            0:
                1.0
        }

        for z, p_z in (
            value_event_probs.items()
        ):

            bit = value_to_bit[
                z
            ]

            new_block_dist: Dict[
                int,
                float
            ] = defaultdict(float)

            for (
                old_mask,
                old_p
            ) in block_dist.items():

                new_block_dist[
                    old_mask
                ] += (
                    old_p
                    *
                    (
                        1.0
                        -
                        p_z
                    )
                )

                new_block_dist[
                    old_mask
                    |
                    bit
                ] += (
                    old_p
                    *
                    p_z
                )

            block_dist = dict(
                new_block_dist
            )

        new_dist: Dict[
            int,
            float
        ] = defaultdict(float)

        for old_mask, old_p in dist.items():

            for (
                block_mask,
                block_p
            ) in block_dist.items():

                new_dist[
                    old_mask
                    |
                    block_mask
                ] += (
                    old_p
                    *
                    block_p
                )

        dist = dict(
            new_dist
        )

    total = sum(
        dist.values()
    )

    if total > 0.0:

        for key in list(
            dist.keys()
        ):

            dist[key] /= total

    return (
        dist,
        evaluations
    )


# ============================================================
# PROBABILITY OF A COMMON eid
# ============================================================

def probability_common_join_value(
    dist_r1: Dict[int, float],
    dist_r2: Dict[int, float],
) -> float:

    p = 0.0

    for m1, p1 in dist_r1.items():

        for m2, p2 in dist_r2.items():

            if (
                m1
                &
                m2
            ) != 0:

                p += (
                    p1
                    *
                    p2
                )

    return clamp_probability(
        p
    )


# ============================================================
# GLOBAL VIOLATION DIAGNOSTICS
#
# These quantities are retained because they were reported in
# the experiments:
#
#     P1(U_none)
#     P1(U_both)
#     P1(U)
#
# The global event is not sent as one large formula to the
# compiler. Instead, small block-level probabilities are
# evaluated by ProvSQL and combined using block independence.
#
# Note:
# At large scale these diagnostics are expensive because
# every block must be examined.
# ============================================================

def all_block_ids_from_rows(
    rows: List[Dict[str, str]]
) -> List[str]:

    return sorted(
        {
            str(
                row[
                    "block_id"
                ]
            )
            for row in rows
        },
        key=
            numericish_sort_key
    )


def compile_u_none_block_sql(
    relation_name: str,
    block_id: str,
    relation_specs: Dict[str, Dict[str, Any]],
) -> str:

    spec = relation_specs[
        relation_name
    ]

    alias = (
        f"{spec['rel_prefix'].upper()}_unone"
    )

    return (
        f"{BOOLEAN_EVENT_SQL}\n"
        "FROM (\n"
        f"    SELECT vn.block_id\n"
        f"    FROM "
        f"provsql_test.{spec['vnone_table']} vn\n"
        f"    WHERE "
        f"vn.block_id = {sql_quote(block_id)}\n"
        "\n"
        "    EXCEPT\n"
        "\n"
        f"    SELECT p.block_id\n"
        f"    FROM "
        f"provsql_test.{spec['prime_table']} p\n"
        f"    WHERE "
        f"p.block_id = {sql_quote(block_id)}\n"
        f"    GROUP BY p.block_id\n"
        f") {alias}"
    )


def compile_u_both_block_sql(
    relation_name: str,
    block_id: str,
    relation_specs: Dict[str, Dict[str, Any]],
) -> str:

    spec = relation_specs[
        relation_name
    ]

    alias = (
        f"{spec['rel_prefix'].upper()}_uboth"
    )

    return (
        f"{BOOLEAN_EVENT_SQL}\n"
        "FROM (\n"
        f"    SELECT vb.block_id\n"
        f"    FROM "
        f"provsql_test.{spec['vboth_table']} vb\n"
        f"    JOIN "
        f"provsql_test.{spec['prime_table']} t1\n"
        f"      ON "
        f"vb.tuple_id_1 = t1.tuple_id\n"
        f"    JOIN "
        f"provsql_test.{spec['prime_table']} t2\n"
        f"      ON "
        f"vb.tuple_id_2 = t2.tuple_id\n"
        f"    WHERE "
        f"vb.block_id = {sql_quote(block_id)}\n"
        f") {alias}"
    )


def compute_global_violation_summary_from_provsql(
    cur,
    rows_by_relation: Dict[
        str,
        List[Dict[str, str]]
    ],
    relation_specs: Dict[str, Dict[str, Any]],
) -> Dict[str, float]:

    prod_not_u_none = 1.0

    prod_not_u_both = 1.0

    prod_not_u = 1.0

    for relation_name in [
        "R1",
        "R2"
    ]:

        rows = rows_by_relation[
            relation_name
        ]

        block_ids = all_block_ids_from_rows(
            rows
        )

        for block_id in block_ids:

            u_none_sql = (
                compile_u_none_block_sql(
                    relation_name,
                    block_id,
                    relation_specs
                )
            )

            u_both_sql = (
                compile_u_both_block_sql(
                    relation_name,
                    block_id,
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

            p1_u_none = clamp_probability(
                p1_E(
                    cur,
                    u_none_sql
                )
            )

            p1_u_both = clamp_probability(
                p1_E(
                    cur,
                    u_both_sql
                )
            )

            p1_u = clamp_probability(
                p1_E(
                    cur,
                    u_sql
                )
            )

            prod_not_u_none *= (
                1.0
                -
                p1_u_none
            )

            prod_not_u_both *= (
                1.0
                -
                p1_u_both
            )

            prod_not_u *= (
                1.0
                -
                p1_u
            )

    return {
        "p1_u_none":
            clamp_probability(
                1.0
                -
                prod_not_u_none
            ),

        "p1_u_both":
            clamp_probability(
                1.0
                -
                prod_not_u_both
            ),

        "p1_u":
            clamp_probability(
                1.0
                -
                prod_not_u
            ),
    }


# ============================================================
# QUERY-DEPENDENT RESTRICTED MARKOVIEWS
# ============================================================

def p_Q(
    cur,
    parsed: Dict[str, Any],
    rows_r1: List[Dict[str, str]],
    rows_r2: List[Dict[str, str]],
    relation_specs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:

    join_attr = parsed[
        "join_attr"
    ]

    # --------------------------------------------------------
    # Query-dependent relevant eids / blocks.
    # --------------------------------------------------------

    r1_by_value = (
        matching_blocks_by_join_value(
            rows_r1,
            parsed[
                "conds_r1"
            ],
            join_attr
        )
    )

    r2_by_value = (
        matching_blocks_by_join_value(
            rows_r2,
            parsed[
                "conds_r2"
            ],
            join_attr
        )
    )

    join_values = sorted(
        set(
            r1_by_value.keys()
        )
        &
        set(
            r2_by_value.keys()
        ),
        key=
            numericish_sort_key
    )

    # --------------------------------------------------------
    # Global diagnostics used in the reported experiments.
    # --------------------------------------------------------

    violation_summary = (
        compute_global_violation_summary_from_provsql(
            cur=
                cur,

            rows_by_relation={
                "R1":
                    rows_r1,

                "R2":
                    rows_r2,
            },

            relation_specs=
                relation_specs,
        )
    )

    p1_u_none = (
        violation_summary[
            "p1_u_none"
        ]
    )

    p1_u_both = (
        violation_summary[
            "p1_u_both"
        ]
    )

    p1_u = (
        violation_summary[
            "p1_u"
        ]
    )

    # --------------------------------------------------------
    # No common relevant eid => query probability 0.
    # --------------------------------------------------------

    if not join_values:

        return {
            "join_values":
                [],

            "r1_by_value":
                r1_by_value,

            "r2_by_value":
                r2_by_value,

            "p1_q":
                0.0,

            "p1_u_none":
                p1_u_none,

            "p1_u_both":
                p1_u_both,

            "p1_u":
                p1_u,

            "p1_q_or_u":
                p1_u,

            "p_exact":
                0.0,
        }

    # --------------------------------------------------------
    # P1(Q) under the auxiliary TID.
    # --------------------------------------------------------

    (
        dist_r1_p0,
        _
    ) = (
        compute_relation_join_value_distribution_p0_provsql(
            cur=
                cur,

            relation_name=
                "R1",

            rows=
                rows_r1,

            conds=
                parsed[
                    "conds_r1"
                ],

            join_attr=
                join_attr,

            join_values=
                join_values,

            relation_specs=
                relation_specs,
        )
    )

    (
        dist_r2_p0,
        _
    ) = (
        compute_relation_join_value_distribution_p0_provsql(
            cur=
                cur,

            relation_name=
                "R2",

            rows=
                rows_r2,

            conds=
                parsed[
                    "conds_r2"
                ],

            join_attr=
                join_attr,

            join_values=
                join_values,

            relation_specs=
                relation_specs,
        )
    )

    p1_q = probability_common_join_value(
        dist_r1_p0,
        dist_r2_p0
    )

    # --------------------------------------------------------
    # BID-corrected query-dependent evaluation.
    # --------------------------------------------------------

    (
        dist_r1,
        _
    ) = (
        compute_relation_join_value_distribution(
            cur=
                cur,

            relation_name=
                "R1",

            rows=
                rows_r1,

            conds=
                parsed[
                    "conds_r1"
                ],

            join_attr=
                join_attr,

            join_values=
                join_values,

            relation_specs=
                relation_specs,
        )
    )

    (
        dist_r2,
        _
    ) = (
        compute_relation_join_value_distribution(
            cur=
                cur,

            relation_name=
                "R2",

            rows=
                rows_r2,

            conds=
                parsed[
                    "conds_r2"
                ],

            join_attr=
                join_attr,

            join_values=
                join_values,

            relation_specs=
                relation_specs,
        )
    )

    p_exact = probability_common_join_value(
        dist_r1,
        dist_r2
    )

    # --------------------------------------------------------
    # Diagnostic P1(Q OR U)
    #
    # P0(Q)
    # =
    # [P1(Q OR U) - P1(U)]
    # ----------------------
    #        1 - P1(U)
    #
    # Rearranged:
    #
    # P1(Q OR U)
    # =
    # P1(U) + P0(Q)(1-P1(U))
    # --------------------------------------------------------

    p1_q_or_u = clamp_probability(
        p1_u
        +
        p_exact
        *
        (
            1.0
            -
            p1_u
        )
    )

    return {
        "join_values":
            join_values,

        "r1_by_value":
            r1_by_value,

        "r2_by_value":
            r2_by_value,

        "p1_q":
            p1_q,

        "p1_u_none":
            p1_u_none,

        "p1_u_both":
            p1_u_both,

        "p1_u":
            p1_u,

        "p1_q_or_u":
            p1_q_or_u,

        "p_exact":
            p_exact,
    }


# ============================================================
# PRINT RESULT
# ============================================================

def print_boolean_result(
    summary: Dict[str, Any],
    preprocess_time: float
):

    print(
        "\nProbabilities:"
    )

    print()

    print(
        "P1(Q)      = "
        f"{format_probability(summary['p1_q'])}"
    )

    print(
        "P1(U_none) = "
        f"{format_probability(summary['p1_u_none'])}"
    )

    print(
        "P1(U_both) = "
        f"{format_probability(summary['p1_u_both'])}"
    )

    print(
        "P1(U)      = "
        f"{format_probability(summary['p1_u'])}"
    )

    print(
        "P1(Q or U) = "
        f"{format_probability(summary['p1_q_or_u'])}"
    )

    print(
        "\n===== RESULT ====="
    )

    print(
        "P(Q) exact = "
        f"{format_probability(summary['p_exact'])}"
    )

    print(
        "Preprocess time =",
        round(
            preprocess_time,
            4
        ),
        "sec"
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
            "Query-Dependent Restricted MarkoViews "
            "evaluation over BID relations using ProvSQL."
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
        help=(
            "Print the query-relevant eid/block sets."
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

    relations = [
        {
            "query_name":
                "R1",

            "csv_path":
                str(r1_path),

            "block_column":
                "block_id",

            "project_columns": [
                "eid",
                "Education",
                "Experience",
                "Position",
            ],

            "weight_column":
                "probability",
        },

        {
            "query_name":
                "R2",

            "csv_path":
                str(r2_path),

            "block_column":
                "block_id",

            "project_columns": [
                "eid",
                "Position",
                "SalaryBand",
            ],

            "weight_column":
                "probability",
        },
    ]

    relation_specs = (
        build_relation_specs()
    )

    relation_inputs = {}

    rows_by_relation: Dict[
        str,
        List[Dict[str, str]]
    ] = {}

    # --------------------------------------------------------
    # Read and translate the BID relations.
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
            f"{rel_name} from:\n"
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
        ] = built[
            "projected_rows"
        ]

    conn = connect()

    try:

        # ----------------------------------------------------
        # Verify environment.
        # ----------------------------------------------------

        with conn.cursor() as cur:

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

            connection_info = (
                cur.fetchone()
            )

            print(
                "\nConnected as:",
                connection_info
            )

            if (
                connection_info[0]
                != "tutorial"
            ):

                raise RuntimeError(
                    "Wrong database. "
                    "Expected tutorial."
                )

            if (
                connection_info[2]
                != "1.12.0"
            ):

                raise RuntimeError(
                    "Wrong ProvSQL version. "
                    "Expected 1.12.0."
                )

            check_installed_sql(
                cur
            )

        # ----------------------------------------------------
        # Upload and preprocess.
        # ----------------------------------------------------

        preprocess_time = 0.0

        if REBUILD_INPUTS_AND_PREPROCESS:

            with conn.cursor() as cur:

                cur.execute(
                    """
                    CREATE SCHEMA
                    IF NOT EXISTS
                    provsql_test;
                    """
                )

                cur.execute(
                    """
                    SET search_path TO
                        provsql_test,
                        provsql,
                        public;
                    """
                )

            conn.commit()

            for (
                rel_name,
                built
            ) in relation_inputs.items():

                spec = relation_specs[
                    rel_name
                ]

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
                    "  prime rows =",
                    len(
                        built[
                            "prime_rows"
                        ]
                    )
                )

                print(
                    "  vboth rows =",
                    len(
                        built[
                            "vboth_rows"
                        ]
                    )
                )

                print(
                    "  vnone rows =",
                    len(
                        built[
                            "vnone_rows"
                        ]
                    )
                )

            print(
                "\nRunning SQL preprocess functions..."
            )

            start = time.time()

            run_preprocess(
                conn
            )

            preprocess_time = (
                time.time()
                -
                start
            )

            print(
                "Preprocess finished in "
                f"{round(preprocess_time, 4)} sec"
            )

        # ----------------------------------------------------
        # Query input.
        # ----------------------------------------------------

        print(
            "\nEnter the Boolean SQL query."
        )

        print(
            "Press Enter immediately to use "
            "the default thesis query:"
        )

        print(
            DEFAULT_USER_SQL
        )

        lines = []

        while True:

            line = input()

            if not line.strip():
                break

            lines.append(
                line.rstrip()
            )

        user_sql = (
            "\n".join(lines)
            .strip()
        )

        if not user_sql:

            user_sql = (
                DEFAULT_USER_SQL
            )

            print(
                "Using the default Boolean query."
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
        # Query-dependent restricted evaluation.
        # ----------------------------------------------------

        with conn.cursor() as cur:

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

        if (
            DEBUG
            or
            args.debug
        ):

            print(
                "\n===== QUERY-DEPENDENT "
                "RESTRICTED MARKOVIEWS DETAILS ====="
            )

            print(
                "Relevant eids:",
                summary[
                    "join_values"
                ]
            )

            print(
                "\nR1 relevant blocks by eid:"
            )

            for (
                value,
                blocks
            ) in (
                summary[
                    "r1_by_value"
                ].items()
            ):

                print(
                    f"  {value}: {blocks}"
                )

            print(
                "\nR2 relevant blocks by eid:"
            )

            for (
                value,
                blocks
            ) in (
                summary[
                    "r2_by_value"
                ].items()
            ):

                print(
                    f"  {value}: {blocks}"
                )

        print_boolean_result(
            summary,
            preprocess_time
        )

    finally:

        conn.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
