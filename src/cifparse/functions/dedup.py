"""
group_safe_insert.py
~~~~~~~~~~~~~~~~~~~~
Skip inserting a batch if *any* row for the same group key (X, Y, Z …) is
already present.

Functions
---------
fields_before(fields, pivot='seq_no')  -> list[str]
bulk_insert_if_group_new(conn, *, table, rows, key_fields) -> bool
"""

from __future__ import annotations
import sqlite3
from typing import Iterable, Mapping, Sequence
from collections import defaultdict

# ----------------------------------------------------------------------
# 1)  Leading-sorted helper  -------------------------------------------
# ----------------------------------------------------------------------

def fields_before(fields: Sequence[str], pivot: str = "seq_no") -> list[str]:
    """
    Return the sub-list that appears *before* `pivot`.

    >>> fields_before(['center_id', 'seq_no', 'start_min'])
    ['center_id']
    """
    try:
        return list(fields[: fields.index(pivot)])
    except ValueError:
        return list(fields)               # pivot not present → keep all


def _unique_indexes(cur: sqlite3.Cursor, table: str) -> list[tuple[str, ...]]:
    """Return a list of UNIQUE index column tuples for `table`."""
    uniques: list[tuple[str, ...]] = []
    for row in cur.execute(f"PRAGMA index_list('{table}')"):
        seq, idx_name, is_unique = row[:3]  # ← ignore extras
        if is_unique:                        # 1 → UNIQUE
            cols = cur.execute(f"PRAGMA index_info('{idx_name}')").fetchall()
            uniques.append(tuple(col[2] for col in cols))  # col[2] == name
    return uniques

def build_select_one(table: str, columns: list[str], values: tuple):
    """
    Return a (sql, params) pair where any column whose value is None
    is compared with `IS ?`, otherwise with `= ?`.

    Example
    -------
    >>> cols = ["st","area","sec_code","sub_code",
    ...         "environment_id","environment_region",
    ...         "environment_sub_code","waypoint_id","waypoint_region"]
    >>> vals = ('S','PAC','E','A','ENRT',None,None,'NOHEA','PH')
    >>> sql, params = build_select_one("waypoints", cols, vals)
    >>> cur.execute(sql, params).fetchone()
    """
    if len(columns) != len(values):
        raise ValueError("columns and values must have the same length")

    where_parts = []
    final_values = []
    for col, val in zip(columns, values):
        if val is None:
            continue
        op = "IS" if val is None else "="
        where_parts.append(f"{col} {op} ?")
        final_values.append(val)

    sql = f"SELECT * FROM {table} WHERE " + " AND ".join(where_parts) + " LIMIT 1"
    return sql, tuple(final_values)  # values go straight through; None binds fine

# ----------------------------------------------------------------------
# 2)  Group-aware bulk insert  -----------------------------------------
# ----------------------------------------------------------------------

def bulk_insert_if_group_new(
    cur: sqlite3.Cursor,
    *,
    table: str,
    rows: Iterable[Mapping[str, object]],
    key_fields: Sequence[str],
) -> bool:
    return insert_groups_with_conflict_report(cur, table=table, rows=rows, group_fields=key_fields)
    """
    Insert rows in `rows`, but *only* for those key-groups that do **not**
    already exist in `table`.

    A “group” is the set of rows sharing identical values for every column
    listed in `key_fields`.

    Returns
    -------
    bool
        True  → at least one row was inserted
        False → no rows inserted (every group already existed)
    """
    rows = list(rows)  # need to iterate twice
    if not rows:
        return False

    _param_limit = 999

    unique_idxs = _unique_indexes(cur, table) # debugging

    # 1. Bucket rows by their composite key ------------------------------
    groups: dict[tuple, list[Mapping[str, object]]] = defaultdict(list)
    for r in rows:
        groups[tuple(r[k] for k in key_fields)].append(r)

    key_width = len(key_fields)
    batch_sz = max(1, _param_limit // key_width)

    existing_keys: set[tuple] = set()
    col_list = ", ".join(key_fields)

    # ------------------------------------------------------------------
    # 2. Ask DB which of those keys already exist
    #    Build one OR-chained WHERE clause:
    #       (k1=? AND k2=? ...) OR (k1=? AND k2=? ...) ...
    # ------------------------------------------------------------------
    # 2. In batches, ask which keys already exist ------------------------
    all_keys = list(groups.keys())
    for i in range(0, len(all_keys), batch_sz):
        chunk = all_keys[i: i + batch_sz]

        values_clause = ", ".join(
            f"({', '.join('?' * key_width)})" for _ in chunk
        )
        params = [v for key in chunk for v in key]

        sql = (
            f"WITH keys({col_list}) AS (VALUES {values_clause}) "
            f"SELECT {col_list} "
            f"FROM   keys "
            f"JOIN   {table} USING ({col_list})"
        )
        existing_keys.update(tuple(row) for row in cur.execute(sql, params))

    # ------------------------------------------------------------------
    # 3. Filter rows whose group key is new
    # ------------------------------------------------------------------
    rows_to_insert = [
        row for key, grp_rows in groups.items() if key not in existing_keys
        for row in grp_rows
    ]
    if not rows_to_insert:                     # nothing new → done
        return False

    # ------------------------------------------------------------------
    # 4. Bulk-insert the remaining rows
    # ------------------------------------------------------------------
    all_cols = list(rows_to_insert[0].keys())
    col_list = ", ".join(all_cols)
    placeholders = ", ".join(f":{c}" for c in all_cols)
    insert_sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

    with cur.connection:
        cur.executemany(insert_sql, rows_to_insert)

    return True


alphabetToPhoentic = {
    'A': 'ALPHA',
    'B': 'BRAVO',
    'C': 'CHARLIE',
    'D': 'DELTA',
    'E': 'ECHO',
    'F': 'FOXTROT',
    'G': 'GOLF',
    'H': 'HOTEL',
    'I': 'INDIA',
    'J': 'JULIETT',
    'K': 'KILO',
    'L': 'LIMA',
    'M': 'MIKE',
    'N': 'NOVEMBER',
    'O': 'OSCAR',
    'P': 'PAPA',
    'Q': 'QUEBEC',
    'R': 'ROMEO',
    'S': 'SIERRA',
    'T': 'TANGO',
    'U': 'UNIFORM',
    'V': 'VICTOR',
    'W': 'WHISKEY',
    'X': 'XRAY',
    'Y': 'YANKEE',
    'Z': 'ZULU'
}


def insert_groups_with_conflict_report(
    cur: sqlite3.Cursor,
    *,
    table: str,
    rows: Iterable[Mapping[str, object]],
    group_fields: Sequence[str],           # defines “a group”
) -> bool:
    """
    Insert rows group-by-group, skipping any group whose key already exists.

    When a UNIQUE conflict still happens (inside-batch dupes or race),
    print the offending rows *and* the rows that already exist.
    """
    rows = list(rows)
    if not rows:
        return False

    # discover full UNIQUE indexes once
    unique_indexes = _unique_indexes(cur, table)

    # put incoming rows into buckets keyed by the *group* columns
    groups = defaultdict(list)
    for r in rows:
        groups[tuple(r[c] for c in group_fields)].append(r)

    # param-prepared INSERT for all columns
    cols = list(rows[0].keys())
    col_csv = ", ".join(cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    insert_sql = f"""
        INSERT OR IGNORE INTO {table} ({col_csv})
        VALUES ({placeholders})
    """

    select_group_sql = (
        f"SELECT * FROM {table} WHERE "
        + " AND ".join(f"{c}=?" for c in group_fields)
    )

    for gkey, grp_rows in groups.items():
        # -------- pre-check: does this group already exist? ------------
        gkeys = [gkey]
        gdict = dict(zip(group_fields, gkey))  # materialise for easy access

        id_field = [c for c in group_fields if c.endswith("_id")]

        # Restrictive airspace in the US have multiple ways to be encoded, so we need to check all possible variations
        # But US doesn't encode the 'K' or 'S' region prefixes, or the 'A/W/P/R' type prefixes, that EAD does for US restrictive
        if 'restrictive' in table and id_field and gdict.get(id_field[0]):
            id_field = id_field[0]
            _id = gdict.get(id_field)
            name_field = [c for c in grp_rows[0] if c.endswith("_name")]
            name_field = name_field[0] if name_field else None
            _name = grp_rows[0].get(name_field) if name_field else None

            if _id and (_id[0] == 'K' or _id[0] == 'S'):  # EAD MOAs are like 'KSRENO'
                copy = gdict.copy()
                copy[id_field] = _id[1:]  # remove 'K' or 'S' prefix
                gkeys.append(list(copy.values()))
                if len(_id) > 1 and (_id[1] == 'S' or _id[1] == 'R' or _id[1] == 'A' or _id[1] == 'P' or _id[1] == 'W'):  # EAD MOAs are like 'KSRENO', R/W are 'K12345' or 'KR283', A 'KA381'
                    copy = gdict.copy()
                    copy[id_field] = _id[2:]  # remove 'S' prefix etc
                    gkeys.append(list(copy.values()))

                # use "copy" from here so we cover either case above
                if _id[-1] == 'N':
                    copy = copy.copy()
                    copy[id_field] = copy[id_field][:-1] + " NORTH"  # remove 'N' suffix
                    gkeys.append(list(copy.values()))
                elif _id[-1] == 'E':
                    copy = copy.copy()
                    copy[id_field] = copy[id_field][:-1] + " EAST"  # remove 'N' suffix
                    gkeys.append(list(copy.values()))
                elif _id[-1] == 'W':
                    copy = copy.copy()
                    copy[id_field] = copy[id_field][:-1] + " WEST"  # remove 'N' suffix
                    gkeys.append(list(copy.values()))
                elif _id[-1] == 'S':
                    copy = copy.copy()
                    copy[id_field] = copy[id_field][:-1] + " SOUTH"  # remove 'N' suffix
                    gkeys.append(list(copy.values()))

                else:
                    # alphabetic suffixes
                    copy = copy.copy()
                    copy[id_field] = copy[id_field][:-1] + " " + alphabetToPhoentic.get(_id[-1], _id[-1])
                    gkeys.append(list(copy.values()))

                    # erroneous suffixes
                    if gkey[1] == 'USA':
                        copy = copy.copy()
                        copy[id_field] = copy[id_field][:-1]
                        gkeys.append(list(copy.values()))

                if 'HIH' in _id:
                    copy = copy.copy()
                    copy[id_field] = _id.replace('HIH', ' HIGH')
                    gkeys.append(list(copy.values()))
                if 'HIGH' in _id:
                    copy = copy.copy()
                    copy[id_field] = _id.replace('HIGH', ' HIGH')
                    gkeys.append(list(copy.values()))
                elif 'LOW' in _id:
                    copy = copy.copy()
                    copy[id_field] = _id.replace('LOW', ' LOW')
                    gkeys.append(list(copy.values()))

            elif 'HIH' in _id:
                copy = gdict.copy()
                copy[id_field] = _id.replace('HIH', ' HIGH')
                gkeys.append(list(copy.values()))
            elif 'LOW' in _id:
                copy = gdict.copy()
                copy[id_field] = _id.replace('LOW', ' LOW')
                gkeys.append(list(copy.values()))

        # check
        exists = False
        for gkey in gkeys:
            sql, params = build_select_one(table, group_fields, gkey)
            if cur.execute(sql, params).fetchone():
                # skip silently or log, your choice
                exists = True
                break

            # otherwise compare without id_field and use name_field before any commo
            elif len(gkeys) > 1 and id_field and isinstance(id_field, str) and _name and isinstance(name_field, str):
                # make sure we don't have an exact match though and this is just a new mult_code
                sql, params = build_select_one(table, group_fields[:-1], gkey[:-1])
                if cur.execute(sql, params).fetchone():
                    break

                # otherwise check for other foreign matches
                fieldsKeys = {}
                for field in group_fields:
                    if '_id' not in field and '_type' not in field and 'mult_code' not in field:
                        fieldsKeys[field] = grp_rows[0].get(field)

                sql, params = build_select_one(table, list(fieldsKeys.keys()), tuple(fieldsKeys.values()))
                sql = sql.split(' LIMIT')[0] + f" AND {name_field} LIKE '%' || ? || '%' LIMIT 1"  # SQLite string concat
                params = params + (_name.split(',')[0],)  # extend the tuple
                if cur.execute(sql, params).fetchone():
                    exists = True
                    break
        if exists:
            print(f"✓ group {gkey} already exists, skipping insert")
            continue

        # -------- attempt insert for this one group --------------------
        with cur.connection:
            rows_inserted = cur.executemany(insert_sql, grp_rows).rowcount

        if rows_inserted == len(grp_rows):
            # no conflict at all
            continue

        # -------- conflict detected – diagnose -------------------------
        print(f"✗ conflict in group {gkey}: "
              f"{len(grp_rows) - rows_inserted} row(s) ignored"
              f"even after {sql} {params} returned no rows")

        # 1. what we tried
        for r in grp_rows:
            print("  attempted:", r)

        # 2. what’s already there for *this group*
        existing = cur.execute(select_group_sql, gkey).fetchall()
        cols_in_db = [d[0] for d in cur.description]
        for row in existing:
            print("  existing :", dict(zip(cols_in_db, row)))

        # 3. optional: inspect against FULL unique indexes to see which
        #    column(s) actually collided (helps spot “seq_no” surprises)
        for uniq_cols in unique_indexes:
            where = " AND ".join(f"{c}=?" for c in uniq_cols)
            attempted_keys = {tuple(r[c] for c in uniq_cols) for r in grp_rows}
            for uk in attempted_keys:
                dupe = cur.execute(
                    f"SELECT * FROM {table} WHERE {where}", uk
                ).fetchone()
                if dupe:
                    print("    ↳ clashes on UNIQUE",
                          uniq_cols, "=", uk)
                    print("      offending row:", dict(zip(cols_in_db, dupe)))
    return True


# ----------------------------------------------------------------------
# 3)  Tiny demo  --------------------------------------------------------
# ----------------------------------------------------------------------
if __name__ == "__main__":
    con = sqlite3.connect(":memory:")
    con.executescript("""
        CREATE TABLE time_of_op (
            x TEXT, y TEXT, z TEXT, seq_no INTEGER,
            dow INTEGER, start_min INTEGER, end_min INTEGER
        );
    """)

    batch = [
        dict(x="A", y="B", z="C", seq_no=1, dow=2, start_min=600, end_min=720),
        dict(x="A", y="B", z="C", seq_no=2, dow=3, start_min=800, end_min=900),
    ]

    ok1 = bulk_insert_if_group_new(
        con, table="time_of_op", rows=batch, key_fields=("x", "y", "z")
    )
    print("first insert:", ok1)              # → True

    ok2 = bulk_insert_if_group_new(
        con, table="time_of_op", rows=batch, key_fields=("x", "y", "z")
    )
    print("second insert:", ok2)             # → False (skipped)
