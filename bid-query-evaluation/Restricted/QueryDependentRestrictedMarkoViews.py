import csv
import re
import time
from collections import defaultdict
from itertools import combinations
from typing import Any, Dict, List, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_values

# ------------------------------------------------------------------
# User settings
# ------------------------------------------------------------------

RELATIONS = [
    {
        "query_name": "R1",
        "csv_path": "/path/to/Pos_Edu_Exp_expanded.csv",
        "block_column": "block_id",
        "project_columns": ["Education", "Experience", "Position"],
        "weight_column": "probability",
    },
    {
        "query_name": "R2",
        "csv_path": "/path/to/Pos_Sb_Ds_expanded.csv",
        "block_column": "block_id",
        "project_columns": ["Position", "SalaryBand"],
        "weight_column": "probability",
    },
]

DB_CONFIG = {
    "host": "localhost",
    "port": 55433,
    "dbname": "test",
    "user": "postgres",
    "password": "YOUR_POSTGRESQL_PASSWORD",
}

DEBUG = False
REBUILD_INPUTS_AND_PREPROCESS = True
SELECTED_BLOCK_IDS_PER_RELATION = None
MAX_BLOCKS_PER_RELATION = None  # Use None for full data.
ROUND_CLOSE_TO_ONE_EPS = 1e-12

# ProvSQL helper expects every event query to return a column named k.
BOOLEAN_EVENT_SQL = "SELECT TRUE AS k"

DEFAULT_USER_SQL = """SELECT TRUE AS k
FROM R1 r1
JOIN R2 r2
ON r1.Position = r2.Position
WHERE r1.Education = 'PhD'
AND r1.Experience = '0-15'
AND r2.SalaryBand = '121k+';"""



def connect():
    return psycopg2.connect(**DB_CONFIG)


def normalize_header(name: str) -> str:
    return str(name).strip().lower()


def sanitize_token(value: Any) -> str:
    value = str(value).strip().replace(" ", "_")
    value = re.sub(r"[^A-Za-z0-9_]+", "", value)
    return value if value else "EMPTY"


def sql_quote(value: Any) -> str:
    s = str(value)
    return "'" + s.replace("'", "''") + "'"


def numericish_sort_key(value: Any):
    s = str(value).strip()
    if re.fullmatch(r"-?\d+", s):
        return (0, int(s))
    return (1, s)


def make_tuple_id(relation_name: str, block_value: str, local_index: int) -> str:
    return f"{sanitize_token(relation_name)}__{sanitize_token(block_value)}__{local_index}"


def format_probability(x: float, digits: int = 17) -> str:
    """
    Print probabilities with high precision instead of rounding to 12 decimals.

    The .17g format shows enough digits to distinguish double-precision
    floating-point values.
    """
    return format(float(x), f".{digits}g")




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
    """
    Reads one BID CSV and constructs:

        prime_input, vboth_input, vnone_input

    The BID weight w is translated into the auxiliary TID probability:

        p0 = w / (1 + w)
    """
    block_col = normalize_header(block_column)
    weight_col = normalize_header(weight_column)
    proj_cols = [normalize_header(c) for c in project_columns]

    expected = {block_col, weight_col, *proj_cols}
    selected_blocks_seen = set()
    selected_block_ids_set = None if not selected_block_ids else {str(x) for x in selected_block_ids}

    grouped_weights: Dict[Tuple[str, Tuple[str, ...]], float] = defaultdict(float)

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        if reader.fieldnames is None:
            raise ValueError(f"CSV file has no header row: {csv_path}")

        normalized_fieldnames = {normalize_header(h) for h in reader.fieldnames}

        if not expected.issubset(normalized_fieldnames):
            raise ValueError(
                "CSV is missing required columns.\n"
                f"CSV: {csv_path}\n"
                f"Expected at least: {sorted(expected)}\n"
                f"Found: {reader.fieldnames}"
            )

        for raw_row in reader:
            row = {
                normalize_header(k): ("" if v is None else str(v).strip())
                for k, v in raw_row.items()
            }

            block_value = row[block_col]

            if selected_block_ids is not None:
                if block_value not in selected_block_ids_set:
                    continue
            elif max_blocks is not None:
                if block_value not in selected_blocks_seen:
                    if len(selected_blocks_seen) >= max_blocks:
                        continue
                    selected_blocks_seen.add(block_value)

            w = float(row[weight_col])
            if w < 0.0:
                raise ValueError(f"BID weight must be non-negative; got {w} in block {block_value}")

            projected_values = tuple(row[col] for col in proj_cols)
            grouped_weights[(block_value, projected_values)] += w

    projected_rows: List[Dict[str, str]] = []

    for (block_value, projected_values), summed_w in grouped_weights.items():
        if abs(1.0 - summed_w) <= round_close_to_one_eps:
            summed_w = 1.0

        row = {"block_id": block_value, "weight": str(summed_w)}
        for i, col in enumerate(proj_cols):
            row[col] = projected_values[i]
        projected_rows.append(row)

    projected_rows.sort(
        key=lambda r: (
            numericish_sort_key(r["block_id"]),
            *(str(r[c]) for c in proj_cols),
        )
    )

    prime_rows = []
    vboth_rows = []
    vnone_rows = []

    tuples_per_block: Dict[str, List[str]] = defaultdict(list)
    seen_blocks = set()

    for row in projected_rows:
        block_value = row["block_id"]
        projected_values = [row[c] for c in proj_cols]
        w = float(row["weight"])

        p0 = w / (1.0 + w)

        local_index = len(tuples_per_block[block_value]) + 1
        tuple_id = make_tuple_id(relation_name, block_value, local_index)

        prime_rows.append(tuple([tuple_id, block_value] + projected_values + [p0]))
        tuples_per_block[block_value].append(tuple_id)

        if block_value not in seen_blocks:
            vnone_rows.append((block_value, 1.0))
            seen_blocks.add(block_value)

    for block_value, tuple_ids in tuples_per_block.items():
        for t1, t2 in combinations(tuple_ids, 2):
            vboth_rows.append((block_value, t1, t2, 1.0))

    return {
        "projected_rows": projected_rows,
        "ordinary_attributes": proj_cols,
        "prime_rows": prime_rows,
        "vboth_rows": vboth_rows,
        "vnone_rows": vnone_rows,
    }




def upload_relation_inputs(
        cur,
        rel_prefix: str,
        ordinary_attrs: List[str],
        prime_rows,
        vboth_rows,
        vnone_rows,
):
    cur.execute(f"DROP TABLE IF EXISTS provsql_test.{rel_prefix}_prime_input CASCADE;")
    cur.execute(f"DROP TABLE IF EXISTS provsql_test.{rel_prefix}_vboth_input CASCADE;")
    cur.execute(f"DROP TABLE IF EXISTS provsql_test.{rel_prefix}_vnone_input CASCADE;")

    ordinary_cols_sql = ",\n            ".join(f"{col} TEXT" for col in ordinary_attrs)

    cur.execute(
        f"""
        CREATE TABLE provsql_test.{rel_prefix}_prime_input (
            tuple_id TEXT,
            block_id TEXT,
            {ordinary_cols_sql},
            p0 DOUBLE PRECISION
        );
        """
    )

    cur.execute(
        f"""
        CREATE TABLE provsql_test.{rel_prefix}_vboth_input (
            block_id TEXT,
            tuple_id_1 TEXT,
            tuple_id_2 TEXT,
            p0 DOUBLE PRECISION
        );
        """
    )

    cur.execute(
        f"""
        CREATE TABLE provsql_test.{rel_prefix}_vnone_input (
            block_id TEXT,
            p0 DOUBLE PRECISION
        );
        """
    )

    prime_columns = ["tuple_id", "block_id"] + ordinary_attrs + ["p0"]

    execute_values(
        cur,
        f"INSERT INTO provsql_test.{rel_prefix}_prime_input ({', '.join(prime_columns)}) VALUES %s",
        prime_rows,
        template="(" + ", ".join(["%s"] * len(prime_columns)) + ")",
    )

    if vboth_rows:
        execute_values(
            cur,
            f"INSERT INTO provsql_test.{rel_prefix}_vboth_input "
            f"(block_id, tuple_id_1, tuple_id_2, p0) VALUES %s",
            vboth_rows,
            template="(%s, %s, %s, %s)",
        )

    execute_values(
        cur,
        f"INSERT INTO provsql_test.{rel_prefix}_vnone_input (block_id, p0) VALUES %s",
        vnone_rows,
        template="(%s, %s)",
    )


def run_preprocess(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT provsql_test.preprocess_r1();")
        cur.execute("SELECT provsql_test.preprocess_r2();")
    conn.commit()


def check_installed_sql(cur):
    cur.execute(
        """
        SELECT COUNT(*)
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'provsql_test'
          AND p.proname IN ('preprocess_r1','preprocess_r2','p1_e');
        """
    )

    if cur.fetchone()[0] < 3:
        raise RuntimeError(
            "Run the SQL script first: it must define preprocess_r1, "
            "preprocess_r2, and p1_E."
        )


def p1_E(cur, event_sql: str) -> float:
    cur.execute("SELECT provsql_test.p1_E(%s);", (event_sql,))
    value = cur.fetchone()[0]
    return 0.0 if value is None else float(value)


# User SQL parsing


def normalize_sql_text(sql: str) -> str:
    return (
        sql.replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
    )


def strip_trailing_semicolon(sql: str) -> str:
    return normalize_sql_text(sql).strip().rstrip(";").strip()


def split_where_conditions(where_sql: str) -> List[str]:
    parts = []
    buf = []
    depth = 0
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
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1

            if depth == 0:
                if s[i:i + 2].upper() == "OR":
                    before = s[i - 1] if i > 0 else " "
                    after = s[i + 2] if i + 2 < len(s) else " "
                    if before.isspace() and after.isspace():
                        raise ValueError(
                            "OR conditions in the WHERE clause are not supported. "
                            "Only AND-connected equality conditions are allowed."
                        )

                if s[i:i + 3].upper() == "AND":
                    before = s[i - 1] if i > 0 else " "
                    after = s[i + 3] if i + 3 < len(s) else " "
                    if before.isspace() and after.isspace():
                        parts.append("".join(buf).strip())
                        buf = []
                        i += 3
                        continue

        buf.append(ch)
        i += 1

    if buf:
        parts.append("".join(buf).strip())

    return [p for p in parts if p]


def parse_select_clause(
        select_sql: str,
        alias_r1: str,
        alias_r2: str,
        relation_specs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Supported forms:

      SELECT TRUE AS k

      SELECT DISTINCT r1.block_id
      SELECT DISTINCT r2.block_id

    Projection queries are treated as marginal answer queries:
    one probability is computed for each returned projected value.
    """
    select_clean = " ".join(select_sql.strip().split())

    if re.fullmatch(r"TRUE\s+AS\s+k", select_clean, flags=re.IGNORECASE):
        return {
            "select_kind": "boolean",
            "projection_relation": None,
            "projection_attr": None,
            "distinct": False,
        }

    projection_match = re.fullmatch(
        r"(DISTINCT\s+)?([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)",
        select_clean,
        flags=re.IGNORECASE,
    )

    if not projection_match:
        raise ValueError(
            "Unsupported SELECT clause. Supported forms are:\n"
            "  SELECT TRUE AS k ...\n"
            "  SELECT DISTINCT r1.block_id ...\n"
            "  SELECT DISTINCT r2.block_id ..."
        )

    is_distinct = projection_match.group(1) is not None
    alias = projection_match.group(2)
    attr = normalize_header(projection_match.group(3))

    if not is_distinct:
        raise ValueError(
            "Projection queries must use SELECT DISTINCT, for example:\n"
            "  SELECT DISTINCT r1.block_id ..."
        )

    if alias.lower() == alias_r1.lower():
        relation_name = "R1"
    elif alias.lower() == alias_r2.lower():
        relation_name = "R2"
    else:
        raise ValueError(
            f"Unknown SELECT alias '{alias}'. Expected '{alias_r1}' or '{alias_r2}'."
        )

    valid_attrs = ["block_id"] + relation_specs[relation_name]["ordinary_attributes"]

    if attr not in valid_attrs:
        raise ValueError(
            f"Unknown SELECT attribute '{attr}' for {relation_name}. "
            f"Allowed: {valid_attrs}"
        )

    return {
        "select_kind": "projection",
        "projection_relation": relation_name,
        "projection_attr": attr,
        "distinct": True,
    }


def parse_sql_user_query(sql: str, relation_specs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    s = strip_trailing_semicolon(sql)

    pattern = re.compile(
        r"""
        ^\s*SELECT\s+
        (?P<select>.+?)\s+
        FROM\s+R1\s+(?P<alias_r1>[A-Za-z_][A-Za-z0-9_]*)\s+
        JOIN\s+R2\s+(?P<alias_r2>[A-Za-z_][A-Za-z0-9_]*)\s+
        ON\s+(?P<on>.+?)\s+
        WHERE\s+(?P<where>.+)\s*$
        """,
        flags=re.IGNORECASE | re.DOTALL | re.VERBOSE,
    )

    m = pattern.match(s)

    if not m:
        raise ValueError(
            "Unsupported SQL format. Use one of these forms:\n"
            "  SELECT TRUE AS k FROM R1 r1 JOIN R2 r2 ON r1.Position = r2.Position WHERE ...\n"
            "  SELECT DISTINCT r1.block_id FROM R1 r1 JOIN R2 r2 ON r1.Position = r2.Position WHERE ..."
        )

    select_sql = m.group("select").strip()
    alias_r1 = m.group("alias_r1")
    alias_r2 = m.group("alias_r2")
    on_sql = m.group("on").strip()
    where_sql = m.group("where").strip()

    select_info = parse_select_clause(
        select_sql=select_sql,
        alias_r1=alias_r1,
        alias_r2=alias_r2,
        relation_specs=relation_specs,
    )

    on_match = re.fullmatch(
        rf"{re.escape(alias_r1)}\.([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"
        rf"{re.escape(alias_r2)}\.([A-Za-z_][A-Za-z0-9_]*)",
        on_sql,
        flags=re.IGNORECASE,
    )

    if not on_match:
        raise ValueError("Only a single equality join of the form r1.attr = r2.attr is supported.")

    join_left = normalize_header(on_match.group(1))
    join_right = normalize_header(on_match.group(2))

    if join_left != join_right:
        raise ValueError("This evaluator expects the join to use the same attribute name on both sides.")

    if join_left not in relation_specs["R1"]["ordinary_attributes"]:
        raise ValueError(f"Join attribute '{join_left}' is not valid for R1.")

    if join_right not in relation_specs["R2"]["ordinary_attributes"]:
        raise ValueError(f"Join attribute '{join_right}' is not valid for R2.")

    conds_r1: List[Tuple[str, str]] = []
    conds_r2: List[Tuple[str, str]] = []

    for cond in split_where_conditions(where_sql):
        m1 = re.fullmatch(
            rf"{re.escape(alias_r1)}\.([A-Za-z_][A-Za-z0-9_]*)\s*=\s*'(.*)'",
            cond,
            flags=re.IGNORECASE | re.DOTALL,
        )

        m2 = re.fullmatch(
            rf"{re.escape(alias_r2)}\.([A-Za-z_][A-Za-z0-9_]*)\s*=\s*'(.*)'",
            cond,
            flags=re.IGNORECASE | re.DOTALL,
        )

        if m1:
            attr = normalize_header(m1.group(1))
            val = m1.group(2).replace("''", "'")

            if attr == "block_id":
                raise ValueError("Do not use block_id in the observed query WHERE clause.")

            if attr not in relation_specs["R1"]["ordinary_attributes"]:
                raise ValueError(f"Unknown R1 attribute '{attr}'.")

            conds_r1.append((attr, val))
            continue

        if m2:
            attr = normalize_header(m2.group(1))
            val = m2.group(2).replace("''", "'")

            if attr == "block_id":
                raise ValueError("Do not use block_id in the observed query WHERE clause.")

            if attr not in relation_specs["R2"]["ordinary_attributes"]:
                raise ValueError(f"Unknown R2 attribute '{attr}'.")

            conds_r2.append((attr, val))
            continue

        raise ValueError(
            f"Unsupported WHERE condition: {cond!r}. "
            "Only simple equality conditions of the form alias.attr = 'value' are supported."
        )

    return {
        "select_kind": select_info["select_kind"],
        "projection_relation": select_info["projection_relation"],
        "projection_attr": select_info["projection_attr"],
        "distinct": select_info["distinct"],
        "alias_r1": alias_r1,
        "alias_r2": alias_r2,
        "join_attr": join_left,
        "conds_r1": conds_r1,
        "conds_r2": conds_r2,
        "user_sql": s,
    }


def row_satisfies_conditions(row: Dict[str, str], conds: List[Tuple[str, str]]) -> bool:
    for attr, val in conds:
        key = normalize_header(attr)
        if key not in row or str(row[key]) != str(val):
            return False
    return True


def conds_to_where_sql(conds: List[Tuple[str, str]], alias: str, valid_columns: List[str]) -> str:
    parts = []

    for attr, val in conds:
        attr = normalize_header(attr)

        if attr not in valid_columns:
            raise ValueError(f"Unknown attribute '{attr}'. Allowed: {valid_columns}")

        parts.append(f"{alias}.{attr} = {sql_quote(val)}")

    return " AND ".join(parts) if parts else "TRUE"




def build_relation_specs() -> Dict[str, Dict[str, Any]]:
    return {
        "R1": {
            "rel_prefix": "r1",
            "prime_table": "r1_prime",
            "vboth_table": "r1_vboth",
            "vnone_table": "r1_vnone",
            "ordinary_attributes": ["education", "experience", "position"],
        },
        "R2": {
            "rel_prefix": "r2",
            "prime_table": "r2_prime",
            "vboth_table": "r2_vboth",
            "vnone_table": "r2_vnone",
            "ordinary_attributes": ["position", "salaryband"],
        },
    }




def compile_global_q_sql(
        parsed: Dict[str, Any],
        relation_specs: Dict[str, Dict[str, Any]],
        projection_filter: Optional[Dict[str, str]] = None,
) -> str:
    """
    Compiles Q as a Boolean event.

    For Boolean queries:
        Q = the event that the query is true.

    For projection queries:
        Q_a = the event that one specific projected answer value a is returned.
    """
    join_attr = parsed["join_attr"]

    r1_where = conds_to_where_sql(
        parsed["conds_r1"],
        "r1",
        relation_specs["R1"]["ordinary_attributes"],
    )

    r2_where = conds_to_where_sql(
        parsed["conds_r2"],
        "r2",
        relation_specs["R2"]["ordinary_attributes"],
    )

    parts = [
        BOOLEAN_EVENT_SQL,
        "FROM provsql_test.r1_prime r1",
        "JOIN provsql_test.r2_prime r2",
        f"  ON r1.{join_attr} = r2.{join_attr}",
        "WHERE TRUE",
    ]

    if r1_where != "TRUE":
        parts.append(f"  AND {r1_where}")

    if r2_where != "TRUE":
        parts.append(f"  AND {r2_where}")

    if projection_filter is not None:
        rel = projection_filter["relation"]
        attr = projection_filter["attr"]
        value = projection_filter["value"]
        alias = "r1" if rel == "R1" else "r2"

        if attr == "block_id":
            parts.append(f"  AND {alias}.block_id = {sql_quote(value)}")
        else:
            if attr not in relation_specs[rel]["ordinary_attributes"]:
                raise ValueError(f"Invalid projection filter attribute '{attr}' for {rel}.")
            parts.append(f"  AND {alias}.{attr} = {sql_quote(value)}")

    return "\n".join(parts)


def compile_u_none_relation_sql(
        relation_name: str,
        relation_specs: Dict[str, Dict[str, Any]],
) -> str:
    spec = relation_specs[relation_name]
    unone_alias = f"{spec['rel_prefix'].upper()}_unone"

    return (
        f"{BOOLEAN_EVENT_SQL}\n"
        "FROM (\n"
        f"    SELECT vn.block_id\n"
        f"    FROM provsql_test.{spec['vnone_table']} vn\n"
        "\n"
        "    EXCEPT\n"
        "\n"
        f"    SELECT p.block_id\n"
        f"    FROM provsql_test.{spec['prime_table']} p\n"
        f"    GROUP BY p.block_id\n"
        f") {unone_alias}"
    )


def compile_u_both_relation_sql(
        relation_name: str,
        relation_specs: Dict[str, Dict[str, Any]],
) -> str:
    spec = relation_specs[relation_name]
    uboth_alias = f"{spec['rel_prefix'].upper()}_uboth"

    return (
        f"{BOOLEAN_EVENT_SQL}\n"
        "FROM (\n"
        f"    SELECT vb.block_id\n"
        f"    FROM provsql_test.{spec['vboth_table']} vb\n"
        f"    JOIN provsql_test.{spec['prime_table']} t1\n"
        f"      ON vb.tuple_id_1 = t1.tuple_id\n"
        f"    JOIN provsql_test.{spec['prime_table']} t2\n"
        f"      ON vb.tuple_id_2 = t2.tuple_id\n"
        f") {uboth_alias}"
    )


def compile_global_u_none_sql(relation_specs: Dict[str, Dict[str, Any]]) -> str:
    return (
            compile_u_none_relation_sql("R1", relation_specs)
            + "\nUNION ALL\n"
            + compile_u_none_relation_sql("R2", relation_specs)
    )


def compile_global_u_both_sql(relation_specs: Dict[str, Dict[str, Any]]) -> str:
    return (
            compile_u_both_relation_sql("R1", relation_specs)
            + "\nUNION ALL\n"
            + compile_u_both_relation_sql("R2", relation_specs)
    )


def compile_global_u_sql(relation_specs: Dict[str, Dict[str, Any]]) -> str:
    return (
            compile_global_u_none_sql(relation_specs)
            + "\nUNION ALL\n"
            + compile_global_u_both_sql(relation_specs)
    )


def exact_probability_for_event_sql(
        cur,
        q_sql: str,
        u_sql: str,
        p1_u: Optional[float] = None,
) -> Dict[str, float]:
    q_or_u_sql = q_sql + "\nUNION ALL\n" + u_sql

    p1_q = p1_E(cur, q_sql)

    if p1_u is None:
        p1_u = p1_E(cur, u_sql)

    p1_q_or_u = p1_E(cur, q_or_u_sql)

    denom = 1.0 - p1_u
    p_exact = 0.0 if abs(denom) < 1e-15 else (p1_q_or_u - p1_u) / denom

    if p_exact < 0.0 and abs(p_exact) < 1e-12:
        p_exact = 0.0

    if p_exact > 1.0 and abs(p_exact - 1.0) < 1e-12:
        p_exact = 1.0

    return {
        "p1_q": p1_q,
        "p1_u": p1_u,
        "p1_q_or_u": p1_q_or_u,
        "p_exact": p_exact,
        "q_or_u_sql": q_or_u_sql,
    }


def compute_boolean_probability_summary(
        cur,
        parsed: Dict[str, Any],
        relation_specs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    q_sql = compile_global_q_sql(parsed, relation_specs)

    u_none_sql = compile_global_u_none_sql(relation_specs)
    u_both_sql = compile_global_u_both_sql(relation_specs)
    u_sql = u_none_sql + "\nUNION ALL\n" + u_both_sql

    p1_u_none = p1_E(cur, u_none_sql)
    p1_u_both = p1_E(cur, u_both_sql)

    event_res = exact_probability_for_event_sql(
        cur=cur,
        q_sql=q_sql,
        u_sql=u_sql,
        p1_u=None,
    )

    return {
        "p1_q": event_res["p1_q"],
        "p1_u_none": p1_u_none,
        "p1_u_both": p1_u_both,
        "p1_u": event_res["p1_u"],
        "p1_q_or_u": event_res["p1_q_or_u"],
        "p_exact": event_res["p_exact"],
        "q_sql": q_sql,
        "u_none_sql": u_none_sql,
        "u_both_sql": u_both_sql,
        "u_sql": u_sql,
        "q_or_u_sql": event_res["q_or_u_sql"],
    }




def candidate_projection_values(
        parsed: Dict[str, Any],
        rows_r1: List[Dict[str, str]],
        rows_r2: List[Dict[str, str]],
) -> List[str]:
    """
    Finds candidate output values for:

        SELECT DISTINCT r1.block_id ...
        SELECT DISTINCT r2.block_id ...

    using the possible tuples after projection.
    """
    if parsed["select_kind"] != "projection":
        return []

    projection_relation = parsed["projection_relation"]
    projection_attr = parsed["projection_attr"]
    join_attr = parsed["join_attr"]

    values = set()

    for r1 in rows_r1:
        if not row_satisfies_conditions(r1, parsed["conds_r1"]):
            continue

        for r2 in rows_r2:
            if not row_satisfies_conditions(r2, parsed["conds_r2"]):
                continue

            if str(r1.get(join_attr, "")) != str(r2.get(join_attr, "")):
                continue

            if projection_relation == "R1":
                values.add(str(r1[projection_attr]))
            else:
                values.add(str(r2[projection_attr]))

    return sorted(values, key=numericish_sort_key)


def compute_projection_probability_table(
        cur,
        parsed: Dict[str, Any],
        rows_r1: List[Dict[str, str]],
        rows_r2: List[Dict[str, str]],
        relation_specs: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Computes marginal probabilities for each projected answer value.

    Example:

        SELECT DISTINCT r1.block_id
        FROM R1 r1 JOIN R2 r2 ...
        WHERE ...

    Output:

        block_id | probability
        3        | 0.084
    """
    projection_relation = parsed["projection_relation"]
    projection_attr = parsed["projection_attr"]

    answer_values = candidate_projection_values(
        parsed=parsed,
        rows_r1=rows_r1,
        rows_r2=rows_r2,
    )

    if not answer_values:
        return []

    u_sql = compile_global_u_sql(relation_specs)
    p1_u = p1_E(cur, u_sql)

    output_rows = []

    for value in answer_values:
        q_value_sql = compile_global_q_sql(
            parsed=parsed,
            relation_specs=relation_specs,
            projection_filter={
                "relation": projection_relation,
                "attr": projection_attr,
                "value": value,
            },
        )

        event_res = exact_probability_for_event_sql(
            cur=cur,
            q_sql=q_value_sql,
            u_sql=u_sql,
            p1_u=p1_u,
        )

        output_rows.append(
            {
                projection_attr: value,
                "probability": event_res["p_exact"],
                "p1_q": event_res["p1_q"],
                "p1_u": event_res["p1_u"],
                "p1_q_or_u": event_res["p1_q_or_u"],
                "q_sql": q_value_sql,
            }
        )

    return output_rows




def compile_q(
        relation_name: str,
        block_id: str,
        join_attr: str,
        join_value: str,
        conds: List[Tuple[str, str]],
        relation_specs: Dict[str, Dict[str, Any]],
) -> str:
    spec = relation_specs[relation_name]
    where = conds_to_where_sql(conds, "p", spec["ordinary_attributes"])

    parts = [
        BOOLEAN_EVENT_SQL,
        f"FROM provsql_test.{spec['prime_table']} p",
        f"WHERE p.block_id = {sql_quote(block_id)}",
        f"  AND p.{join_attr} = {sql_quote(join_value)}",
    ]

    if where != "TRUE":
        parts.append(f"  AND {where}")

    return "\n".join(parts)


def compile_u(
        relation_name: str,
        block_id: str,
        relation_specs: Dict[str, Dict[str, Any]],
) -> str:
    spec = relation_specs[relation_name]
    unone_alias = f"{spec['rel_prefix'].upper()}_unone"

    u_none = (
        f"{BOOLEAN_EVENT_SQL}\n"
        "FROM (\n"
        f"    SELECT vn.block_id\n"
        f"    FROM provsql_test.{spec['vnone_table']} vn\n"
        f"    WHERE vn.block_id = {sql_quote(block_id)}\n"
        "\n"
        "    EXCEPT\n"
        "\n"
        f"    SELECT p.block_id\n"
        f"    FROM provsql_test.{spec['prime_table']} p\n"
        f"    WHERE p.block_id = {sql_quote(block_id)}\n"
        f"    GROUP BY p.block_id\n"
        f") {unone_alias}"
    )

    uboth_alias = f"{spec['rel_prefix'].upper()}_uboth"

    u_both = (
        f"{BOOLEAN_EVENT_SQL}\n"
        "FROM (\n"
        f"    SELECT vb.block_id\n"
        f"    FROM provsql_test.{spec['vboth_table']} vb\n"
        f"    JOIN provsql_test.{spec['prime_table']} t1\n"
        f"      ON vb.tuple_id_1 = t1.tuple_id\n"
        f"    JOIN provsql_test.{spec['prime_table']} t2\n"
        f"      ON vb.tuple_id_2 = t2.tuple_id\n"
        f"    WHERE vb.block_id = {sql_quote(block_id)}\n"
        f") {uboth_alias}"
    )

    return u_none + "\nUNION ALL\n" + u_both


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
        relation_name=relation_name,
        block_id=block_id,
        join_attr=join_attr,
        join_value=join_value,
        conds=conds,
        relation_specs=relation_specs,
    )

    u_sql = compile_u(relation_name, block_id, relation_specs)
    event_res = exact_probability_for_event_sql(cur, q_sql, u_sql)

    return {
        "relation": relation_name,
        "block_id": block_id,
        "join_value": join_value,
        "q_sql": q_sql,
        "u_sql": u_sql,
        "q_or_u_sql": event_res["q_or_u_sql"],
        "p1_q": event_res["p1_q"],
        "p1_u": event_res["p1_u"],
        "p1_q_or_u": event_res["p1_q_or_u"],
        "p_bid": event_res["p_exact"],
    }


def matching_blocks_by_join_value(
        rows: List[Dict[str, str]],
        conds: List[Tuple[str, str]],
        join_attr: str,
) -> Dict[str, List[str]]:
    out: Dict[str, set] = defaultdict(set)

    for row in rows:
        if row_satisfies_conditions(row, conds):
            if join_attr in row:
                out[str(row[join_attr])].add(str(row["block_id"]))

    return {z: sorted(list(blocks), key=numericish_sort_key) for z, blocks in out.items()}


def compute_relation_join_value_distribution(
        cur,
        relation_name: str,
        rows: List[Dict[str, str]],
        conds: List[Tuple[str, str]],
        join_attr: str,
        join_values: List[str],
        relation_specs: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[int, float], List[Dict[str, Any]]]:
    value_to_bit = {z: 1 << i for i, z in enumerate(join_values)}

    block_to_values: Dict[str, set] = defaultdict(set)

    for row in rows:
        z = str(row.get(join_attr, ""))

        if z not in value_to_bit:
            continue

        if not row_satisfies_conditions(row, conds):
            continue

        block_to_values[str(row["block_id"])].add(z)

    block_to_values_sorted = {
        b: sorted(vals, key=lambda x: join_values.index(x))
        for b, vals in block_to_values.items()
    }

    dist: Dict[int, float] = {0: 1.0}
    evaluations: List[Dict[str, Any]] = []

    for block_id in sorted(block_to_values_sorted.keys(), key=numericish_sort_key):
        value_probs: Dict[str, float] = {}

        for z in block_to_values_sorted[block_id]:
            res = exact_mv_probability_for_single_relation_block_value(
                cur=cur,
                relation_name=relation_name,
                block_id=block_id,
                join_attr=join_attr,
                join_value=z,
                conds=conds,
                relation_specs=relation_specs,
            )

            value_probs[z] = res["p_bid"]
            evaluations.append(res)

        p_any_relevant = sum(value_probs.values())

        if p_any_relevant > 1.0 and abs(p_any_relevant - 1.0) < 1e-10:
            p_any_relevant = 1.0

        if p_any_relevant > 1.0 + 1e-8:
            raise RuntimeError(
                f"Invalid block distribution for {relation_name} block {block_id}: "
                f"sum of value probabilities = {p_any_relevant:.15f}."
            )

        block_outcomes = [(0, max(0.0, 1.0 - p_any_relevant))]

        for z, p in value_probs.items():
            block_outcomes.append((value_to_bit[z], p))

        new_dist: Dict[int, float] = defaultdict(float)

        for old_mask, old_p in dist.items():
            for add_mask, add_p in block_outcomes:
                new_dist[old_mask | add_mask] += old_p * add_p

        dist = dict(new_dist)

    total = sum(dist.values())

    if total > 0:
        for k in list(dist.keys()):
            dist[k] = dist[k] / total

    return dist, evaluations


def probability_common_join_value(dist_r1: Dict[int, float], dist_r2: Dict[int, float]) -> float:
    p = 0.0

    for m1, p1 in dist_r1.items():
        for m2, p2 in dist_r2.items():
            if (m1 & m2) != 0:
                p += p1 * p2

    if p < 0 and abs(p) < 1e-12:
        p = 0.0

    if p > 1 and abs(p - 1.0) < 1e-12:
        p = 1.0

    return p


def mask_to_values(mask: int, join_values: List[str]) -> List[str]:
    return [z for i, z in enumerate(join_values) if mask & (1 << i)]


def clamp_probability(x: float, eps: float = 1e-12) -> float:
    if x < 0.0 and abs(x) <= eps:
        return 0.0
    if x > 1.0 and abs(x - 1.0) <= eps:
        return 1.0
    return x


def p0_from_projected_row(row: Dict[str, str]) -> float:
    w = float(row["weight"])
    return w / (1.0 + w)


def relation_violation_summary_from_rows(rows: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Retained only to preserve the original function name.

    In the revised version, violation probabilities are not computed from
    Python rows. They are evaluated by ProvSQL through p1_E(...) using the
    SQL events produced by compile_global_u_none_sql(...),
    compile_global_u_both_sql(...), and compile_global_u_sql(...).
    """
    raise RuntimeError(
        "relation_violation_summary_from_rows is not used in the revised "
        "ProvSQL-only probability path. Use p1_E(cur, u_sql) instead."
    )


def global_violation_summary_from_rows(
        rows_r1: List[Dict[str, str]],
        rows_r2: List[Dict[str, str]],
) -> Dict[str, Any]:
    """
    Retained only to preserve the original function name.

    In the revised version, the global violation event is built as SQL:

        U = U_none UNION ALL U_both

    and P1(U) is obtained by calling p1_E(cur, u_sql), which delegates the
    probability computation to ProvSQL.
    """
    raise RuntimeError(
        "global_violation_summary_from_rows is not used in the revised "
        "ProvSQL-only probability path. Use p1_E(cur, compile_global_u_sql(...)) instead."
    )


def compute_relation_join_value_distribution_p0(
        rows: List[Dict[str, str]],
        conds: List[Tuple[str, str]],
        join_attr: str,
        join_values: List[str],
) -> Dict[int, float]:
    """
    Retained only to preserve the original function name.

    In the revised version, P1(Q) is not computed by a Python dynamic program.
    The query event Q is compiled to SQL by compile_global_q_sql(...), and
    P1(Q) is obtained with p1_E(cur, q_sql), i.e., from ProvSQL.
    """
    raise RuntimeError(
        "compute_relation_join_value_distribution_p0 is not used in the revised "
        "ProvSQL-only probability path. Use p1_E(cur, q_sql) instead."
    )


def all_block_ids_from_rows(rows: List[Dict[str, str]]) -> List[str]:
    """
    Returns all block ids present in the projected relation rows.

    This helper does not compute probabilities. It is only used to know which
    small block-level SQL events should be sent to ProvSQL.
    """
    return sorted({str(row["block_id"]) for row in rows}, key=numericish_sort_key)


def compile_u_none_block_sql(
        relation_name: str,
        block_id: str,
        relation_specs: Dict[str, Dict[str, Any]],
) -> str:
    """
    Builds the one-block U_none event:

        the block exists, but no ordinary tuple from that block is selected.

    Probability is not computed here. The returned SQL is sent to p1_E(...),
    which calls ProvSQL.
    """
    spec = relation_specs[relation_name]
    unone_alias = f"{spec['rel_prefix'].upper()}_unone"

    return (
        f"{BOOLEAN_EVENT_SQL}\n"
        "FROM (\n"
        f"    SELECT vn.block_id\n"
        f"    FROM provsql_test.{spec['vnone_table']} vn\n"
        f"    WHERE vn.block_id = {sql_quote(block_id)}\n"
        "\n"
        "    EXCEPT\n"
        "\n"
        f"    SELECT p.block_id\n"
        f"    FROM provsql_test.{spec['prime_table']} p\n"
        f"    WHERE p.block_id = {sql_quote(block_id)}\n"
        f"    GROUP BY p.block_id\n"
        f") {unone_alias}"
    )


def compile_u_both_block_sql(
        relation_name: str,
        block_id: str,
        relation_specs: Dict[str, Dict[str, Any]],
) -> str:
    """
    Builds the one-block U_both event:

        at least two ordinary tuples from the same block are selected.

    Probability is not computed here. The returned SQL is sent to p1_E(...),
    which calls ProvSQL.
    """
    spec = relation_specs[relation_name]
    uboth_alias = f"{spec['rel_prefix'].upper()}_uboth"

    return (
        f"{BOOLEAN_EVENT_SQL}\n"
        "FROM (\n"
        f"    SELECT vb.block_id\n"
        f"    FROM provsql_test.{spec['vboth_table']} vb\n"
        f"    JOIN provsql_test.{spec['prime_table']} t1\n"
        f"      ON vb.tuple_id_1 = t1.tuple_id\n"
        f"    JOIN provsql_test.{spec['prime_table']} t2\n"
        f"      ON vb.tuple_id_2 = t2.tuple_id\n"
        f"    WHERE vb.block_id = {sql_quote(block_id)}\n"
        f") {uboth_alias}"
    )


def compute_global_violation_summary_from_provsql(
        cur,
        rows_by_relation: Dict[str, List[Dict[str, str]]],
        relation_specs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Computes global P1(U_none), P1(U_both), and P1(U) without sending one
    huge global U formula to D4.

    Each block-level violation probability is evaluated by ProvSQL:

        p1_E(cur, U_none_block_sql)
        p1_E(cur, U_both_block_sql)
        p1_E(cur, U_block_sql)

    Python only combines independent block-level events:

        P(OR_i E_i) = 1 - product_i (1 - P(E_i)).

    This avoids the d4 failure caused by compiling one large global U formula.
    """
    prod_not_u_none = 1.0
    prod_not_u_both = 1.0
    prod_not_u = 1.0
    relation_summaries: Dict[str, Any] = {}

    for relation_name in ["R1", "R2"]:
        rows = rows_by_relation[relation_name]
        block_ids = all_block_ids_from_rows(rows)

        rel_prod_not_u_none = 1.0
        rel_prod_not_u_both = 1.0
        rel_prod_not_u = 1.0
        block_summaries = []

        for block_id in block_ids:
            u_none_sql = compile_u_none_block_sql(relation_name, block_id, relation_specs)
            u_both_sql = compile_u_both_block_sql(relation_name, block_id, relation_specs)
            u_sql = u_none_sql + "\nUNION ALL\n" + u_both_sql

            p1_u_none_b = p1_E(cur, u_none_sql)
            p1_u_both_b = p1_E(cur, u_both_sql)
            p1_u_b = p1_E(cur, u_sql)

            p1_u_none_b = clamp_probability(p1_u_none_b)
            p1_u_both_b = clamp_probability(p1_u_both_b)
            p1_u_b = clamp_probability(p1_u_b)

            rel_prod_not_u_none *= (1.0 - p1_u_none_b)
            rel_prod_not_u_both *= (1.0 - p1_u_both_b)
            rel_prod_not_u *= (1.0 - p1_u_b)

            block_summaries.append(
                {
                    "block_id": block_id,
                    "p1_u_none": p1_u_none_b,
                    "p1_u_both": p1_u_both_b,
                    "p1_u": p1_u_b,
                    "u_none_sql": u_none_sql,
                    "u_both_sql": u_both_sql,
                    "u_sql": u_sql,
                }
            )

        rel_p1_u_none = clamp_probability(1.0 - rel_prod_not_u_none)
        rel_p1_u_both = clamp_probability(1.0 - rel_prod_not_u_both)
        rel_p1_u = clamp_probability(1.0 - rel_prod_not_u)

        relation_summaries[relation_name] = {
            "p1_u_none": rel_p1_u_none,
            "p1_u_both": rel_p1_u_both,
            "p1_u": rel_p1_u,
            "blocks": block_summaries,
        }

        prod_not_u_none *= (1.0 - rel_p1_u_none)
        prod_not_u_both *= (1.0 - rel_p1_u_both)
        prod_not_u *= (1.0 - rel_p1_u)

    return {
        "p1_u_none": clamp_probability(1.0 - prod_not_u_none),
        "p1_u_both": clamp_probability(1.0 - prod_not_u_both),
        "p1_u": clamp_probability(1.0 - prod_not_u),
        "R1": relation_summaries["R1"],
        "R2": relation_summaries["R2"],
    }


def compute_relation_join_value_distribution_p0_provsql(
        cur,
        relation_name: str,
        rows: List[Dict[str, str]],
        conds: List[Tuple[str, str]],
        join_attr: str,
        join_values: List[str],
        relation_specs: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[int, float], List[Dict[str, Any]]]:
    """
    Computes the P1 distribution over join-value masks for one relation.

    Unlike the old compute_relation_join_value_distribution_p0(...), this
    function does not compute event probabilities directly from row p0 values.
    For each block and join value, it builds a small Q_block,value event and
    obtains its probability from ProvSQL through p1_E(...).

    Python only combines the small ProvSQL-computed event probabilities using
    independence of disjoint blocks and disjoint join-value tuple sets.
    """
    value_to_bit = {z: 1 << i for i, z in enumerate(join_values)}

    block_to_values: Dict[str, set] = defaultdict(set)

    for row in rows:
        z = str(row.get(join_attr, ""))

        if z not in value_to_bit:
            continue

        if not row_satisfies_conditions(row, conds):
            continue

        block_to_values[str(row["block_id"])].add(z)

    block_to_values_sorted = {
        b: sorted(vals, key=lambda x: join_values.index(x))
        for b, vals in block_to_values.items()
    }

    dist: Dict[int, float] = {0: 1.0}
    evaluations: List[Dict[str, Any]] = []

    for block_id in sorted(block_to_values_sorted.keys(), key=numericish_sort_key):
        value_event_probs: Dict[str, float] = {}

        for z in block_to_values_sorted[block_id]:
            q_sql = compile_q(
                relation_name=relation_name,
                block_id=block_id,
                join_attr=join_attr,
                join_value=z,
                conds=conds,
                relation_specs=relation_specs,
            )

            p1_q_bz = clamp_probability(p1_E(cur, q_sql))
            value_event_probs[z] = p1_q_bz

            evaluations.append(
                {
                    "relation": relation_name,
                    "block_id": block_id,
                    "join_value": z,
                    "p1_q": p1_q_bz,
                    "q_sql": q_sql,
                }
            )

        block_dist: Dict[int, float] = {0: 1.0}

        for z, p_z in value_event_probs.items():
            bit = value_to_bit[z]
            new_block_dist: Dict[int, float] = defaultdict(float)

            for old_mask, old_p in block_dist.items():
                new_block_dist[old_mask] += old_p * (1.0 - p_z)
                new_block_dist[old_mask | bit] += old_p * p_z

            block_dist = dict(new_block_dist)

        new_dist: Dict[int, float] = defaultdict(float)

        for old_mask, old_p in dist.items():
            for block_mask, block_p in block_dist.items():
                new_dist[old_mask | block_mask] += old_p * block_p

        dist = dict(new_dist)

    total = sum(dist.values())

    if total > 0:
        for k in list(dist.keys()):
            dist[k] = dist[k] / total

    return dist, evaluations


def p_Q(
        cur,
        parsed: Dict[str, Any],
        rows_r1: List[Dict[str, str]],
        rows_r2: List[Dict[str, str]],
        relation_specs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Blockwise ProvSQL Boolean-query evaluator.

    This version avoids the d4 error that appears when one large global Q,
    U, or Q OR U formula is sent to ProvSQL.

    The probability of every primitive event is still obtained from ProvSQL
    through p1_E(...). Python only performs exact independence-based
    combination of those small ProvSQL-computed probabilities.
    """
    join_attr = parsed["join_attr"]

    r1_by_value = matching_blocks_by_join_value(rows_r1, parsed["conds_r1"], join_attr)
    r2_by_value = matching_blocks_by_join_value(rows_r2, parsed["conds_r2"], join_attr)
    join_values = sorted(set(r1_by_value.keys()) & set(r2_by_value.keys()))

    rows_by_relation = {
        "R1": rows_r1,
        "R2": rows_r2,
    }

    # P1(U_none), P1(U_both), and P1(U), computed from small block-level
    # ProvSQL calls and combined by independence across blocks/relations.
    violation_summary = compute_global_violation_summary_from_provsql(
        cur=cur,
        rows_by_relation=rows_by_relation,
        relation_specs=relation_specs,
    )

    p1_u_none = violation_summary["p1_u_none"]
    p1_u_both = violation_summary["p1_u_both"]
    p1_u = violation_summary["p1_u"]

    if not join_values:
        return {
            "join_values": [],
            "r1_by_value": r1_by_value,
            "r2_by_value": r2_by_value,
            "dist_r1": {0: 1.0},
            "dist_r2": {0: 1.0},
            "dist_r1_p0": {0: 1.0},
            "dist_r2_p0": {0: 1.0},
            "evals_r1": [],
            "evals_r2": [],
            "evals_r1_p0": [],
            "evals_r2_p0": [],
            "violation_summary": violation_summary,
            "p1_q": 0.0,
            "p1_u_none": p1_u_none,
            "p1_u_both": p1_u_both,
            "p1_u": p1_u,
            "p1_q_or_u": p1_u,
            "p_exact": 0.0,
            "p_bid": 0.0,
        }

    # P1(Q): computed from small ProvSQL calls for each relation/block/join-value
    # event, then combined exactly using independence.
    dist_r1_p0, evals_r1_p0 = compute_relation_join_value_distribution_p0_provsql(
        cur=cur,
        relation_name="R1",
        rows=rows_r1,
        conds=parsed["conds_r1"],
        join_attr=join_attr,
        join_values=join_values,
        relation_specs=relation_specs,
    )

    dist_r2_p0, evals_r2_p0 = compute_relation_join_value_distribution_p0_provsql(
        cur=cur,
        relation_name="R2",
        rows=rows_r2,
        conds=parsed["conds_r2"],
        join_attr=join_attr,
        join_values=join_values,
        relation_specs=relation_specs,
    )

    p1_q = probability_common_join_value(dist_r1_p0, dist_r2_p0)

    # P(Q): BID-corrected probability. Each block-level value probability is
    # obtained from ProvSQL with P1(Q_b), P1(U_b), and P1(Q_b OR U_b), then
    # corrected by the MarkoView formula inside exact_probability_for_event_sql.
    dist_r1, evals_r1 = compute_relation_join_value_distribution(
        cur=cur,
        relation_name="R1",
        rows=rows_r1,
        conds=parsed["conds_r1"],
        join_attr=join_attr,
        join_values=join_values,
        relation_specs=relation_specs,
    )

    dist_r2, evals_r2 = compute_relation_join_value_distribution(
        cur=cur,
        relation_name="R2",
        rows=rows_r2,
        conds=parsed["conds_r2"],
        join_attr=join_attr,
        join_values=join_values,
        relation_specs=relation_specs,
    )

    p_exact = probability_common_join_value(dist_r1, dist_r2)
    p_exact = clamp_probability(p_exact)

    # Avoid a global Q OR U call to D4. Recover this value exactly from:
    #   P1(Q OR U) = P1(U) + P_BID(Q) * (1 - P1(U)).
    p1_q_or_u = clamp_probability(p1_u + p_exact * (1.0 - p1_u))

    return {
        "join_values": join_values,
        "r1_by_value": r1_by_value,
        "r2_by_value": r2_by_value,
        "dist_r1": dist_r1,
        "dist_r2": dist_r2,
        "dist_r1_p0": dist_r1_p0,
        "dist_r2_p0": dist_r2_p0,
        "evals_r1": evals_r1,
        "evals_r2": evals_r2,
        "evals_r1_p0": evals_r1_p0,
        "evals_r2_p0": evals_r2_p0,
        "violation_summary": violation_summary,
        "p1_q": p1_q,
        "p1_u_none": p1_u_none,
        "p1_u_both": p1_u_both,
        "p1_u": p1_u,
        "p1_q_or_u": p1_q_or_u,
        "p_exact": p_exact,
        "p_bid": p_exact,
    }




def print_boolean_result(summary: Dict[str, Any], preprocess_time: float):
    print("\nProbabilities:")
    print()
    print(f"P1(Q)      = {format_probability(summary['p1_q'])}")
    print(f"P1(U_none) = {format_probability(summary['p1_u_none'])}")
    print(f"P1(U_both) = {format_probability(summary['p1_u_both'])}")
    print(f"P1(U)      = {format_probability(summary['p1_u'])}")
    print(f"P1(Q or U) = {format_probability(summary['p1_q_or_u'])}")

    print("\n\n===== RESULTS =====")
    print(f"P(Q) exact = {format_probability(summary['p_exact'])}")
    print("Preprocess time =", round(preprocess_time, 4), "sec")
    print("===================")


def print_projection_result(
        parsed: Dict[str, Any],
        rows: List[Dict[str, Any]],
        preprocess_time: float,
):
    label = parsed["projection_attr"]

    print("\n===== RESULTS =====")
    print(f"{label} | probability")
    print("----------------------")

    for row in rows:
        print(f"{row[label]} | {format_probability(row['probability'])}")

    print("Preprocess time =", round(preprocess_time, 4), "sec")
    print("===================")




def main():
    relation_specs = build_relation_specs()
    relation_inputs = {}
    rows_by_relation: Dict[str, List[Dict[str, str]]] = {}

    for rel_cfg in RELATIONS:
        rel_name = rel_cfg["query_name"].strip().upper()

        print(f"\nReading and translating {rel_name} from: {rel_cfg['csv_path']}")

        built = build_relation_inputs_from_csv(
            relation_name=rel_name,
            csv_path=rel_cfg["csv_path"],
            block_column=rel_cfg["block_column"],
            project_columns=rel_cfg["project_columns"],
            weight_column=rel_cfg["weight_column"],
            max_blocks=MAX_BLOCKS_PER_RELATION,
            selected_block_ids=SELECTED_BLOCK_IDS_PER_RELATION,
            round_close_to_one_eps=ROUND_CLOSE_TO_ONE_EPS,
        )

        relation_inputs[rel_name] = built
        rows_by_relation[rel_name] = built["projected_rows"]

        # print(f"Projected rows for {rel_name}: {len(built['projected_rows'])}")

    conn = connect()

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user;")
            print("Connected as:", cur.fetchone())
            check_installed_sql(cur)

        preprocess_time = 0.0

        if REBUILD_INPUTS_AND_PREPROCESS:
            with conn.cursor() as cur:
                cur.execute("SET search_path TO provsql_test, provsql, public;")
                cur.execute("CREATE SCHEMA IF NOT EXISTS provsql_test;")
            conn.commit()

            for rel_name, built in relation_inputs.items():
                spec = relation_specs[rel_name]

                with conn.cursor() as cur:
                    upload_relation_inputs(
                        cur,
                        rel_prefix=spec["rel_prefix"],
                        ordinary_attrs=spec["ordinary_attributes"],
                        prime_rows=built["prime_rows"],
                        vboth_rows=built["vboth_rows"],
                        vnone_rows=built["vnone_rows"],
                    )

                conn.commit()

                print(f"\n{rel_name}:")
                print(f"  prime rows = {len(built['prime_rows'])}")
                print(f"  vboth rows = {len(built['vboth_rows'])}")
                print(f"  vnone rows = {len(built['vnone_rows'])}")

            print("\nRunning SQL preprocess functions...")

            t0 = time.time()
            run_preprocess(conn)
            preprocess_time = time.time() - t0

            print(f"Preprocess finished in {round(preprocess_time, 4)} sec")

        print("\nEnter the SQL query.")
        print("If you press Enter without typing a query, the default query below is used:")
        print(DEFAULT_USER_SQL)

        lines = []

        while True:
            line = input()
            if not line.strip():
                break
            lines.append(line.rstrip())

        user_sql = "\n".join(lines).strip()

        if not user_sql:
            user_sql = DEFAULT_USER_SQL
            print("Using the default Boolean join query.")

        parsed = parse_sql_user_query(user_sql, relation_specs)

        print("\nUser SQL:")
        print(parsed["user_sql"] + ";")
        print("\ncomputing...", flush=True)

        with conn.cursor() as cur:
            if parsed["select_kind"] == "boolean":
                summary = p_Q(
                    cur=cur,
                    parsed=parsed,
                    rows_r1=rows_by_relation["R1"],
                    rows_r2=rows_by_relation["R2"],
                    relation_specs=relation_specs,
                )

                if DEBUG:
                    print("\n===== BLOCKWISE BOOLEAN QUERY DETAILS =====")
                    print("Join values:", summary["join_values"])
                    print("\nR1 relevant blocks by join value:")
                    for value, blocks in summary["r1_by_value"].items():
                        print(f"  {value}: {blocks}")
                    print("\nR2 relevant blocks by join value:")
                    for value, blocks in summary["r2_by_value"].items():
                        print(f"  {value}: {blocks}")

                print_boolean_result(summary, preprocess_time)

            elif parsed["select_kind"] == "projection":
                rows = compute_projection_probability_table(
                    cur=cur,
                    parsed=parsed,
                    rows_r1=rows_by_relation["R1"],
                    rows_r2=rows_by_relation["R2"],
                    relation_specs=relation_specs,
                )

                if DEBUG:
                    print("\n===== PROJECTION DETAILS =====")
                    for row in rows:
                        print(row)

                print_projection_result(parsed, rows, preprocess_time)

            else:
                raise RuntimeError(f"Unknown select_kind: {parsed['select_kind']}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
