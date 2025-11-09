#!/usr/bin/env python3
"""
Option B: Full-Python pipeline using SQLite (sqlite3) + pandas.
- Loads the CSVs into a local SQLite database.
- Applies data-quality checks (with helper functions) and writes exception reports to /outputs.
- Creates a transformed, analysis-ready table and CSV.

Usage:
  python etl_sqlite.py --in data --out outputs --db edu.db
"""

import argparse
import os
import sys
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# ---------- Logging & utils ----------
def log(msg):
    print(f"[ETL] {msg}", flush=True)

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def to_sql(conn, df, table):
    df.to_sql(table, conn, if_exists="replace", index=False)

def read_csv_safe(path):
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin-1")

# ---------- Data-quality helper functions ----------
def check_uniqueness(df, column):
    dups = df[df[column].duplicated(keep=False)]
    if not dups.empty:
        print(f"\n[EXC] Duplicate {column} found. Unique offending values:", dups[column].unique())
    else:
        print(f"[OK] {column} is unique.")
    return dups.assign(__reason=f"Duplicate {column}")

def check_nulls(df, cols=None):
    view = df if cols is None else df[cols]
    bad = df[view.isna().any(axis=1)]
    if not bad.empty:
        print(f"[EXC] Nulls in columns: {list(view.columns)} (rows={len(bad)})")
    else:
        print(f"[OK] No nulls in {list(view.columns)}.")
    return bad.assign(__reason="Null values present")

def check_range(df, col, lo=None, hi=None, inclusive="both"):
    s = df[col].astype(float)
    ok = pd.Series(True, index=df.index)
    if lo is not None:
        ok &= s >= lo if inclusive in ("both","left") else s > lo
    if hi is not None:
        ok &= s <= hi if inclusive in ("both","right") else s < hi
    bad = df[~ok]
    if not bad.empty:
        print(f"[EXC] {col} outside [{lo},{hi}] (rows={len(bad)})")
    else:
        print(f"[OK] {col} within range.")
    return bad.assign(__reason=f"{col} outside range")

def check_nonneg(df, col):
    s = df[col].astype(float)
    bad = df[(s < 0) | s.isna()]
    if not bad.empty:
        print(f"[EXC] {col} negative or null (rows={len(bad)})")
    else:
        print(f"[OK] {col} non-negative & non-null.")
    return bad.assign(__reason=f"{col} negative or null")

def check_duplicates(df, subset_cols):
    bad = df[df.duplicated(subset=subset_cols, keep=False)]
    if not bad.empty:
        print(f"[EXC] Duplicate {tuple(subset_cols)} pairs (rows={len(bad)})")
    else:
        print(f"[OK] Unique by {tuple(subset_cols)}.")
    return bad.assign(__reason=f"Duplicate {tuple(subset_cols)}")

def check_fk(child_df, child_col, parent_df, parent_col):
    """Rows in child where FK not found in parent."""
    missing = ~child_df[child_col].isin(parent_df[parent_col].dropna().unique())
    bad = child_df[missing]
    if not bad.empty:
        print(f"[EXC] FK {child_col} → {parent_col} violations (rows={len(bad)})")
    else:
        print(f"[OK] FK {child_col} → {parent_col}")
    return bad.assign(__reason=f"FK {child_col}->{parent_col} missing")

# ---------- Core ETL steps ----------
def run_quality_checks(dfs, out_dir):
    """
    Returns a dict of cleaned dataframes and writes exception reports.
    """
    ensure_dir(out_dir)

    # --- Departments ---
    if 'departments' in dfs:
        d = dfs['departments'].copy()
        ex = []
        if 'Department_ID' in d.columns:
            ex.append(check_uniqueness(d, 'Department_ID'))
        if 'Department_Name' in d.columns:
            ex.append(check_uniqueness(d, 'Department_Name'))
        ex.append(check_nulls(d))
        ex = [e for e in ex if not e.empty]
        if ex:
            pd.concat(ex).drop_duplicates().to_csv(os.path.join(out_dir, "departments_exceptions.csv"), index=False)

        keep_cols = [c for c in ('Department_ID','Department_Name') if c in d.columns]
        dfs['departments_clean'] = d.drop_duplicates(subset=keep_cols) if keep_cols else d

    # --- Students / Counseling ---
    if 'students' in dfs:
        s = dfs['students'].copy()
        ex = [check_nulls(s)]
        if 'Department_ID' in s.columns and 'departments_clean' in dfs and 'Department_ID' in dfs['departments_clean']:
            ex.append(check_fk(s, 'Department_ID', dfs['departments_clean'], 'Department_ID'))
        ex = [e for e in ex if not e.empty]
        if ex:
            pd.concat(ex).drop_duplicates().to_csv(os.path.join(out_dir, "students_exceptions.csv"), index=False)
        dfs['students_clean'] = s.dropna(how='any')

    # --- Performance ---
    if 'performance' in dfs:
        p = dfs['performance'].copy()
        ex = []
        if 'Marks' in p.columns:
            ex.append(check_range(p, 'Marks', lo=0, hi=100))
        if 'Effort_Hours' in p.columns:
            ex.append(check_nonneg(p, 'Effort_Hours'))
        if {'Student_ID','Paper_ID'}.issubset(p.columns):
            ex.append(check_duplicates(p, ['Student_ID','Paper_ID']))
        if 'Student_ID' in p.columns and 'students_clean' in dfs and 'Student_ID' in dfs['students_clean']:
            ex.append(check_fk(p, 'Student_ID', dfs['students_clean'], 'Student_ID'))
        ex = [e for e in ex if not e.empty]
        if ex:
            pd.concat(ex).drop_duplicates().to_csv(os.path.join(out_dir, "performance_exceptions.csv"), index=False)

        pc = p.copy()
        if 'Marks' in pc.columns:
            pc = pc[(pc['Marks'].astype(float) >= 0) & (pc['Marks'].astype(float) <= 100)]
        if 'Effort_Hours' in pc.columns:
            pc = pc[(pc['Effort_Hours'].astype(float) >= 0)]
        if {'Student_ID','Paper_ID'}.issubset(pc.columns):
            pc = pc.drop_duplicates(subset=['Student_ID','Paper_ID'])
        if 'students_clean' in dfs and 'Student_ID' in dfs['students_clean'] and 'Student_ID' in pc.columns:
            pc = pc[pc['Student_ID'].isin(dfs['students_clean']['Student_ID'].dropna().unique())]
        dfs['performance_clean'] = pc

    return dfs

def build_transformed(dfs):
    """
    Create analysis-ready dataset by joining cleaned performance with student & department info.
    """
    perf = dfs.get('performance_clean', dfs.get('performance'))
    if perf is None:
        return None

    out = perf.copy()

    students = dfs.get('students_clean', dfs.get('students'))
    if students is not None and 'Student_ID' in perf.columns and 'Student_ID' in students.columns:
        out = out.merge(students, on='Student_ID', how='left', suffixes=('','_student'))

    departments = dfs.get('departments_clean', dfs.get('departments'))
    if departments is not None and 'Department_ID' in departments.columns and 'Department_ID' in out.columns:
        out = out.merge(departments, on='Department_ID', how='left', suffixes=('','_dept'))

    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_dir", default="data", help="Directory with input CSVs")
    ap.add_argument("--out", dest="out_dir", default="outputs", help="Directory for outputs (exception reports, transformed.csv)")
    ap.add_argument("--db", dest="db_path", default="edu.db", help="SQLite database file to create/overwrite")
    ap.add_argument("--departments_csv", default="Department_Information.csv")
    ap.add_argument("--students_csv", default="Student_Counceling_Information.csv")
    ap.add_argument("--performance_csv", default="Student_Performance_Data.csv")
    ap.add_argument("--employees_csv", default="Employee_Information.csv")
    args = ap.parse_args()

    ensure_dir(args.out_dir)
    conn = sqlite3.connect(args.db_path)

    # --- Load CSVs ---
    paths = {
        "departments": os.path.join(args.in_dir, args.departments_csv),
        "students": os.path.join(args.in_dir, args.students_csv),
        "performance": os.path.join(args.in_dir, args.performance_csv),
        "employees": os.path.join(args.in_dir, args.employees_csv),
    }

    dfs = {}
    for key, path in paths.items():
        if os.path.exists(path):
            log(f"Loading {key} from {path}")
            df = read_csv_safe(path)
            dfs[key] = df
            to_sql(conn, df, key)  # also load raw into SQLite
        else:
            log(f"[WARN] Missing file: {path} (skipping)")

    # --- Quality checks & exceptions ---
    dfs = run_quality_checks(dfs, args.out_dir)

    # --- Build transformed dataset ---
    transformed = build_transformed(dfs)
    if transformed is not None:
        transformed.to_csv(os.path.join(args.out_dir, "transformed.csv"), index=False)
        to_sql(conn, transformed, "fact_performance")
        log(f"Wrote transformed dataset with {len(transformed)} rows.")
    else:
        log("[WARN] No performance data to transform.")

    conn.close()
    log("Done.")

if __name__ == "__main__":
    main()
