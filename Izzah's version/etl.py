#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
etl.py — Part 2 only (kept separate)

Reads these objects (can be tables or views):
  Department_Information, Employee_Information,
  Student_Counceling_Information, Student_Performance_Data

Produces exception CSVs, a cleaned performance CSV,
and a DB table Student_Performance_Data_Clean.

Usage:
  python etl.py --host localhost --port 3306 --db_user root --db_pass "" --db_name edu_sys --out_dir etl_outputs
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


def run_part2_etl(engine, out_dir: str = "etl_outputs"):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Load from DB (tables or views created by the loader)
    with engine.begin() as con:
        dept = pd.read_sql(text("SELECT * FROM Department_Information"), con)
        emp  = pd.read_sql(text("SELECT * FROM Employee_Information"), con)
        stud = pd.read_sql(text("SELECT * FROM Student_Counceling_Information"), con)
        perf = pd.read_sql(text("SELECT * FROM Student_Performance_Data"), con)

    # ---- Department_Information checks ----
    dept_ex = []
    if not dept.empty:
        miss = dept.isna()
        r_idx, c_idx = miss.to_numpy().nonzero()
        for r, c in zip(r_idx, c_idx):
            dept_ex.append({"row_index": int(r),
                            "column": dept.columns[c],
                            "issue_type": "Missing",
                            "details": "null value"})

        if "Department_ID" in dept.columns:
            dup = dept["Department_ID"].duplicated(keep=False)
            for r in dept.index[dup].tolist():
                dept_ex.append({"row_index": int(r),
                                "column": "Department_ID",
                                "issue_type": "Duplicate Key",
                                "details": f"Department_ID={dept.loc[r,'Department_ID']}"})

        if "Department_Name" in dept.columns:
            dup = dept["Department_Name"].duplicated(keep=False)
            for r in dept.index[dup].tolist():
                dept_ex.append({"row_index": int(r),
                                "column": "Department_Name",
                                "issue_type": "Duplicate Value",
                                "details": f"Department_Name={dept.loc[r,'Department_Name']}"})

        if "DOE" in dept.columns:
            for r, v in dept["DOE"].items():
                try:
                    if pd.isna(v) or int(v) < 1900:
                        dept_ex.append({"row_index": int(r),
                                        "column": "DOE",
                                        "issue_type": "Out-of-Range",
                                        "details": f"DOE={v} (must be >=1900)"})
                except Exception:
                    dept_ex.append({"row_index": int(r),
                                    "column": "DOE",
                                    "issue_type": "Invalid Type",
                                    "details": f"DOE={v} (not integer)"})
    pd.DataFrame(dept_ex).to_csv(out / "Department_Information_exceptions.csv", index=False)

    # ---- Employee_Information checks (none required) ----
    pd.DataFrame([], columns=["row_index","column","issue_type","details"]).to_csv(
        out / "Employee_Information_exceptions.csv", index=False
    )

    # ---- Student_Counceling_Information checks ----
    stud_ex = []
    if not stud.empty and "Department_Admission" in stud.columns:
        for r, v in stud["Department_Admission"].items():
            if pd.isna(v) or str(v).strip() == "":
                stud_ex.append({"row_index": int(r),
                                "column": "Department_Admission",
                                "issue_type": "Missing",
                                "details": "Department_Admission is required"})
        dept_ids = set(dept["Department_ID"].dropna().astype(str)) if "Department_ID" in dept.columns else set()
        for r, v in stud["Department_Admission"].items():
            s = str(v).strip()
            if s and dept_ids and s not in dept_ids:
                stud_ex.append({"row_index": int(r),
                                "column": "Department_Admission",
                                "issue_type": "FK Violation",
                                "details": f"Department_Admission={s} not in Department_Information.Department_ID"})
    pd.DataFrame(stud_ex).to_csv(out / "Student_Counceling_Information_exceptions.csv", index=False)

    # ---- Student_Performance_Data checks ----
    perf_ex = []
    discard_idx = set()
    if not perf.empty:
        miss_rows = perf.index[perf.isna().any(axis=1)].tolist()
        for r in miss_rows:
            perf_ex.append({"row_index": int(r), "column": "(row)", "issue_type": "Missing", "details": "Missing values in row"})
        discard_idx.update(miss_rows)

        if "Marks" in perf.columns:
            for r, v in perf["Marks"].items():
                try:
                    f = float(v)
                    if not (0 <= f <= 100):
                        perf_ex.append({"row_index": int(r), "column": "Marks", "issue_type": "Out-of-Range",
                                        "details": f"Marks={v} not in [0,100]"})
                        discard_idx.add(r)
                except Exception:
                    perf_ex.append({"row_index": int(r), "column": "Marks", "issue_type": "Invalid Type",
                                    "details": f"Marks={v} not numeric"})
                    discard_idx.add(r)

        if "Effort_Hours" in perf.columns:
            for r, v in perf["Effort_Hours"].items():
                try:
                    i = int(v)
                    if i < 0 or (isinstance(v, float) and not float(v).is_integer()):
                        perf_ex.append({"row_index": int(r), "column": "Effort_Hours", "issue_type": "Out-of-Range",
                                        "details": f"Effort_Hours={v} must be integer >= 0"})
                        discard_idx.add(r)
                except Exception:
                    perf_ex.append({"row_index": int(r), "column": "Effort_Hours", "issue_type": "Invalid Type",
                                    "details": f"Effort_Hours={v} not integer"})
                    discard_idx.add(r)

        if {"Student_ID","Paper_ID"}.issubset(perf.columns):
            dup_pair = perf.duplicated(subset=["Student_ID","Paper_ID"], keep=False)
            for r in perf.index[dup_pair].tolist():
                pair = (perf.loc[r,"Student_ID"], perf.loc[r,"Paper_ID"])
                perf_ex.append({"row_index": int(r), "column": "Student_ID,Paper_ID", "issue_type": "Duplicate Pair",
                                "details": f"{pair} appears more than once"})

    pd.DataFrame(perf_ex).to_csv(out / "Student_Performance_Data_exceptions.csv", index=False)

    perf_clean = perf.drop(index=sorted(discard_idx)).reset_index(drop=True)
    perf_clean.to_csv(out / "Student_Performance_Data_cleaned.csv", index=False)

    with engine.begin() as con:
        con.execute(text("DROP TABLE IF EXISTS Student_Performance_Data_Clean"))
    perf_clean.to_sql("Student_Performance_Data_Clean", engine, if_exists="replace", index=False)

    summary = pd.DataFrame([
        {"dataset": "Department_Information", "exceptions": len(pd.read_csv(out / "Department_Information_exceptions.csv"))},
        {"dataset": "Employee_Information", "exceptions": len(pd.read_csv(out / "Employee_Information_exceptions.csv"))},
        {"dataset": "Student_Counceling_Information", "exceptions": len(pd.read_csv(out / "Student_Counceling_Information_exceptions.csv"))},
        {"dataset": "Student_Performance_Data", "exceptions": len(pd.read_csv(out / "Student_Performance_Data_exceptions.csv"))},
    ])
    summary.to_csv(out / "ETL_Exception_Summary.csv", index=False)
    print(f"[ETL] Done. Outputs in: {out.resolve()}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--db_user", default="root")
    ap.add_argument("--db_pass", default="")
    ap.add_argument("--db_name", default="edu_sys")
    ap.add_argument("--out_dir", default="etl_outputs")
    args = ap.parse_args()

    url = f"mysql+mysqlconnector://{args.db_user}:{args.db_pass}@{args.host}:{args.port}/{args.db_name}"
    engine = create_engine(url, pool_pre_ping=True)
    run_part2_etl(engine, out_dir=args.out_dir)


if __name__ == "__main__":
    main()
