#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
load_to_mariadb.py — Part 1 + Option A views + optional Part 2 trigger
"""
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

try:
    from etl import run_part2_etl
except Exception:
    run_part2_etl = None


def ensure_db(engine_root: Engine, db_name: str):
    with engine_root.connect() as con:
        con.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name} "
                         "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"))


def engine_from_args(args, with_db=True) -> Engine:
    db = f"/{args.db_name}" if with_db else ""
    url = f"mysql+mysqlconnector://{args.db_user}:{args.db_pass}@{args.host}:{args.port}{db}"
    return create_engine(url, pool_pre_ping=True)


def create_base_schema(engine: Engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS departments (
      department_id   VARCHAR(32) PRIMARY KEY,
      department_name VARCHAR(255) NOT NULL,
      doe             DATE NULL
    );
    CREATE TABLE IF NOT EXISTS employees (
      employee_id   VARCHAR(32) PRIMARY KEY,
      employee_name VARCHAR(255),
      email         VARCHAR(255),
      department_id VARCHAR(32)
    );
    CREATE TABLE IF NOT EXISTS students (
      student_id VARCHAR(32) PRIMARY KEY,
      doa        DATE NULL
    );
    CREATE TABLE IF NOT EXISTS performances (
      paper_id      VARCHAR(32) PRIMARY KEY,
      paper_name    VARCHAR(255),
      semester_name VARCHAR(64)
    );
    CREATE TABLE IF NOT EXISTS admissions (
      student_id    VARCHAR(32),
      department_id VARCHAR(32)
    );
    CREATE TABLE IF NOT EXISTS student_performances (
      student_id VARCHAR(32),
      paper_id   VARCHAR(32),
      mark       FLOAT,
      hours      INT
    );
    """
    with engine.begin() as con:
        for stmt in ddl.split(";"):
            s = stmt.strip()
            if s:
                con.execute(text(s))


def load_from_csvs(engine: Engine, dept_csv: Path, emp_csv: Path, counsel_csv: Path, perf_csv: Path):
    # Departments
    dept = pd.read_csv(dept_csv)
    dept.columns = [c.strip() for c in dept.columns]
    if "DOE" in dept.columns:
        dept["doe"] = pd.to_datetime(dept["DOE"].astype("Int64").astype(str) + "-01-01", errors="coerce")
    else:
        dept["doe"] = pd.NaT
    dept_out = dept.rename(columns={"Department_ID":"department_id","Department_Name":"department_name"})[
        ["department_id","department_name","doe"]
    ]
    dept_out.to_sql("departments", engine, if_exists="replace", index=False)

    # Employees
    emp = pd.read_csv(emp_csv)
    emp.columns = [c.strip() for c in emp.columns]
    emp_out = emp.rename(columns={
        "Employee_ID":"employee_id",
        "Employee_Name":"employee_name",
        "Email":"email",
        "Department_ID":"department_id",
    })
    keep = [c for c in ["employee_id","employee_name","email","department_id"] if c in emp_out.columns]
    emp_out[keep].to_sql("employees", engine, if_exists="replace", index=False)

    # Students & Admissions (from counseling)
    sc = pd.read_csv(counsel_csv)
    sc.columns = [c.strip() for c in sc.columns]
    students = sc.rename(columns={"Student_ID":"student_id"}).copy()
    if "DOA" in students.columns:
        students["doa"] = pd.to_datetime(students["DOA"], errors="coerce")
    else:
        students["doa"] = pd.NaT
    students_out = students[["student_id","doa"]].drop_duplicates("student_id")
    students_out.to_sql("students", engine, if_exists="replace", index=False)

    admissions = sc.rename(columns={"Student_ID":"student_id", "Department_Admission":"department_id"})
    admissions_out = admissions[["student_id","department_id"]]
    admissions_out.to_sql("admissions", engine, if_exists="replace", index=False)

    # Performances & Student_Performances
    sp = pd.read_csv(perf_csv)
    sp.columns = [c.strip() for c in sp.columns]

    perf_tbl = sp.rename(columns={
        "Paper_ID":"paper_id",
        "Paper_Name":"paper_name",
        "Semester_Name":"semester_name"
    })[["paper_id","paper_name","semester_name"]].drop_duplicates("paper_id")
    perf_tbl.to_sql("performances", engine, if_exists="replace", index=False)

    sp_tbl = sp.rename(columns={
        "Student_ID":"student_id",
        "Paper_ID":"paper_id",
        "Marks":"mark",
        "Effort_Hours":"hours"
    })[["student_id","paper_id","mark","hours"]]
    sp_tbl.to_sql("student_performances", engine, if_exists="replace", index=False)


def create_compat_views(engine: Engine):
    sql = """
    CREATE OR REPLACE VIEW Department_Information AS
    SELECT
      d.department_id   AS Department_ID,
      d.department_name AS Department_Name,
      YEAR(d.doe)       AS DOE
    FROM departments d;

    CREATE OR REPLACE VIEW Employee_Information AS
    SELECT
      e.employee_id   AS Employee_ID,
      e.email         AS Email,
      e.department_id AS Department_ID
    FROM employees e;

    CREATE OR REPLACE VIEW Student_Counceling_Information AS
    SELECT
      s.student_id        AS Student_ID,
      NULL                AS Department_Choices,
      s.doa               AS DOA,
      a.department_id     AS Department_Admission,
      NULL                AS Notes
    FROM admissions a
    LEFT JOIN students s ON s.student_id = a.student_id;

    CREATE OR REPLACE VIEW Student_Performance_Data AS
    SELECT
      sp.student_id    AS Student_ID,
      p.semester_name  AS Semester_Name,
      sp.paper_id      AS Paper_ID,
      p.paper_name     AS Paper_Name,
      sp.mark          AS Marks,
      sp.hours         AS Effort_Hours
    FROM student_performances sp
    LEFT JOIN performances p ON p.paper_id = sp.paper_id;
    """
    with engine.begin() as con:
        con.execute(text(sql))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--db_user", default="root")
    ap.add_argument("--db_pass", default="")
    ap.add_argument("--db_name", default="edu_sys")

    ap.add_argument("--dept_csv", required=True)
    ap.add_argument("--emp_csv", required=True)
    ap.add_argument("--counsel_csv", required=True)
    ap.add_argument("--perf_csv", required=True)

    ap.add_argument("--run_part2", action="store_true")
    ap.add_argument("--out_dir", default="etl_outputs")

    args = ap.parse_args()

    root = engine_from_args(args, with_db=False)
    ensure_db(root, args.db_name)
    engine = engine_from_args(args, with_db=True)

    create_base_schema(engine)
    load_from_csvs(engine,
                   Path(args.dept_csv),
                   Path(args.emp_csv),
                   Path(args.counsel_csv),
                   Path(args.perf_csv))

    create_compat_views(engine)
    print("[Loader] Base tables loaded and views created.")

    if args.run_part2:
        if run_part2_etl is None:
            raise RuntimeError("etl.py not found or import failed; place etl.py beside this file.")
        run_part2_etl(engine, out_dir=args.out_dir)


if __name__ == "__main__":
    main()
