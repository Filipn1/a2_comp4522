"""
setup_and_load_edu_sys.py

End-to-end Python setup for the 'edu_sys' database:
- creates DB and user (optional)*
- builds schema matching the ER diagram
- loads CSVs in FK-safe order
- runs integrity checks and prints results

Requires:
    pip install SQLAlchemy mysql-connector-python pandas python-dotenv

Usage:
    python setup_and_load_edu_sys.py \
        --host localhost --port 3306 \
        --root_user root --root_pass "" \
        --db_name edu_sys --db_user edu_user --db_pass edupass \
        --departments_csv ./departments.csv \
        --students_csv ./students.csv \
        --performances_csv ./performances.csv \
        --employees_csv ./employees.csv \
        --admissions_csv ./admissions.csv \
        --student_performances_csv ./student_performances.csv

* If you don't have root access or don't want to create a user,
  create the DB/user once manually and run this script with the
  same --db_user/--db_pass to skip the user creation step.
"""
import argparse
import os
import sys
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from etl import run_part2_etl

def make_engine(user: str, password: str, host: str, port: int, db: Optional[str] = None, autocommit: bool = False) -> Engine:
    db_part = f"/{db}" if db else ""
    # mysql+mysqlconnector works well with MariaDB for SQLAlchemy
    eng = create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}{db_part}",
        pool_pre_ping=True,
        isolation_level="AUTOCOMMIT" if autocommit else None,
    )
    return eng


def create_database_and_user(root_engine: Engine, db_name: str, db_user: str, db_pass: str):
    # Create database if not exists
    root_engine.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name} "
                             "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"))
    # Create user if not exists (MariaDB syntax)
    root_engine.execute(text(
        f"CREATE USER IF NOT EXISTS '{db_user}'@'localhost' IDENTIFIED BY :pwd;"
    ), {"pwd": db_pass})
    root_engine.execute(text(
        f"GRANT ALL PRIVILEGES ON {db_name}.* TO '{db_user}'@'localhost';"
    ))
    root_engine.execute(text("FLUSH PRIVILEGES;"))


DDL = """
-- Drop in dependency order when re-running
DROP TABLE IF EXISTS student_performances;
DROP TABLE IF EXISTS admissions;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS performances;
DROP TABLE IF EXISTS students;
DROP TABLE IF EXISTS departments;

-- Department
CREATE TABLE departments (
  department_id   INT           NOT NULL,
  department_name VARCHAR(150)  NOT NULL,
  doe             DATE          NULL,
  PRIMARY KEY (department_id),
  UNIQUE KEY uq_departments_name (department_name)
) ENGINE=InnoDB;

-- Employee (Works_in → Department)
CREATE TABLE employees (
  employee_id   INT  NOT NULL,
  department_id INT  NOT NULL,
  dob           DATE NULL,
  doj           DATE NULL,
  PRIMARY KEY (employee_id),
  KEY idx_emp_dept (department_id),
  CONSTRAINT fk_emp_department
    FOREIGN KEY (department_id)
    REFERENCES departments(department_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
) ENGINE=InnoDB;

-- Student
CREATE TABLE students (
  student_id INT  NOT NULL,
  dob        DATE NULL,
  doa        DATE NULL,
  PRIMARY KEY (student_id)
) ENGINE=InnoDB;

-- Performance (Paper)
CREATE TABLE performances (
  paper_id       INT           NOT NULL,
  paper_name     VARCHAR(150)  NOT NULL,
  semester_name  VARCHAR(40)   NOT NULL,
  PRIMARY KEY (paper_id),
  UNIQUE KEY uq_paper_semester (paper_name, semester_name)
) ENGINE=InnoDB;

-- Admission (Student ↔ Department) with attribute "choice"
CREATE TABLE admissions (
  student_id    INT          NOT NULL,
  department_id INT          NOT NULL,
  choice        VARCHAR(20)  NULL,
  PRIMARY KEY (student_id, department_id),
  KEY idx_adm_dept (department_id),
  CONSTRAINT fk_adm_student
    FOREIGN KEY (student_id)
    REFERENCES students(student_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  CONSTRAINT fk_adm_department
    FOREIGN KEY (department_id)
    REFERENCES departments(department_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
) ENGINE=InnoDB;

-- Perform (Student ↔ Performance) with attributes "mark", "hours"
CREATE TABLE student_performances (
  student_id INT          NOT NULL,
  paper_id   INT          NOT NULL,
  mark       DECIMAL(5,2) NULL,
  hours      DECIMAL(5,2) NULL,
  PRIMARY KEY (student_id, paper_id),
  KEY idx_perf_paper (paper_id),
  CONSTRAINT fk_sp_student
    FOREIGN KEY (student_id)
    REFERENCES students(student_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  CONSTRAINT fk_sp_paper
    FOREIGN KEY (paper_id)
    REFERENCES performances(paper_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
) ENGINE=InnoDB;
"""


def build_schema(db_engine: Engine):
    with db_engine.begin() as conn:
        for stmt in DDL.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def read_csv_safe(path: str, date_cols=None, numeric_cols=None) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"CSV not found: {path}")
    df = pd.read_csv(path)
    # coerce empties
    if date_cols:
        for c, fmt in date_cols.items():
            if c in df.columns:
                if fmt:
                    df[c] = pd.to_datetime(df[c], format=fmt, errors="coerce").dt.date
                else:
                    df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    if numeric_cols:
        for c in numeric_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def load_tables(db_engine: Engine, args):
    # Load in FK-safe order: departments, students, performances, employees, admissions, student_performances
    loaders = []

    # departments
    dep_df = read_csv_safe(args.departments_csv, date_cols={"doe": None})
    loaders.append(("departments", dep_df))

    # students
    stu_df = read_csv_safe(args.students_csv, date_cols={"dob": None, "doa": None})
    loaders.append(("students", stu_df))

    # performances
    perf_df = read_csv_safe(args.performances_csv)
    loaders.append(("performances", perf_df))

    # employees
    emp_df = read_csv_safe(args.employees_csv, date_cols={"dob": None, "doj": None})
    loaders.append(("employees", emp_df))

    # admissions
    adm_df = read_csv_safe(args.admissions_csv)
    loaders.append(("admissions", adm_df))

    # student_performances
    sp_df = read_csv_safe(args.student_performances_csv, numeric_cols=["mark", "hours"])
    loaders.append(("student_performances", sp_df))

    with db_engine.begin() as conn:
        for table, df in loaders:
            # ensure only columns that exist in the table are sent (helpful if CSVs contain extras)
            df.columns = [c.strip() for c in df.columns]
            df.to_sql(table, conn, if_exists="append", index=False, method="multi", chunksize=1000)
            print(f"Loaded {len(df):>6} rows into {table}")


def run_checks(db_engine: Engine):
    checks = [
        ("PK uniqueness - departments",
         "SELECT COUNT(*) total, COUNT(DISTINCT department_id) distinct_ids FROM departments"),
        ("PK uniqueness - employees",
         "SELECT COUNT(*) total, COUNT(DISTINCT employee_id) distinct_ids FROM employees"),
        ("PK uniqueness - students",
         "SELECT COUNT(*) total, COUNT(DISTINCT student_id) distinct_ids FROM students"),
        ("PK uniqueness - performances",
         "SELECT COUNT(*) total, COUNT(DISTINCT paper_id) distinct_ids FROM performances"),
        ("Orphans in employees (dept missing) - should be 0",
         """SELECT COUNT(*) FROM employees e
            LEFT JOIN departments d ON d.department_id = e.department_id
            WHERE d.department_id IS NULL"""),
        ("Orphans in admissions (student/department missing) - should be 0",
         """SELECT COUNT(*) FROM admissions a
            LEFT JOIN students s   ON s.student_id = a.student_id
            LEFT JOIN departments d ON d.department_id = a.department_id
            WHERE s.student_id IS NULL OR d.department_id IS NULL"""),
        ("Orphans in student_performances (student/paper missing) - should be 0",
         """SELECT COUNT(*) FROM student_performances sp
            LEFT JOIN students s     ON s.student_id  = sp.student_id
            LEFT JOIN performances p ON p.paper_id    = sp.paper_id
            WHERE s.student_id IS NULL OR p.paper_id IS NULL"""),
        ("Duplicate admissions (blocked by PK) - should be 0",
         """SELECT COUNT(*) FROM (
                SELECT student_id, department_id, COUNT(*) c
                FROM admissions GROUP BY 1,2 HAVING c > 1
            ) t"""),
        ("Duplicate student_performances (blocked by PK) - should be 0",
         """SELECT COUNT(*) FROM (
                SELECT student_id, paper_id, COUNT(*) c
                FROM student_performances GROUP BY 1,2 HAVING c > 1
            ) t"""),
    ]
    with db_engine.begin() as conn:
        for label, q in checks:
            val = conn.execute(text(q)).scalar()
            print(f"{label}: {val}")


def main():
    p = argparse.ArgumentParser(description="Build and load edu_sys schema from CSVs")
    # server/admin
    p.add_argument("--host", default=os.getenv("DB_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "3306")))
    p.add_argument("--root_user", default=os.getenv("DB_ROOT_USER", "root"))
    p.add_argument("--root_pass", default=os.getenv("DB_ROOT_PASS", ""))

    # target db/user
    p.add_argument("--db_name", default=os.getenv("DB_NAME", "edu_sys"))
    p.add_argument("--db_user", default=os.getenv("DB_USER", "edu_user"))
    p.add_argument("--db_pass", default=os.getenv("DB_PASS", "edupass"))

    # csv paths
    p.add_argument("--departments_csv", required=True)
    p.add_argument("--students_csv", required=True)
    p.add_argument("--performances_csv", required=True)
    p.add_argument("--employees_csv", required=True)
    p.add_argument("--admissions_csv", required=True)
    p.add_argument("--student_performances_csv", required=True)

    # flags
    p.add_argument("--skip_user_create", action="store_true",
                   help="Skip CREATE USER/GRANT (use if you lack root or user already exists)")

    args = p.parse_args()

    # 1) Root engine (for DB/user creation)
    try:
        root_engine = make_engine(args.root_user, args.root_pass, args.host, args.port, db=None, autocommit=True)
        # touch server to fail fast if wrong creds
        root_engine.execute(text("SELECT 1"))
    except Exception as e:
        print("ERROR: Could not connect with root credentials. "
              "Either provide correct --root_user/--root_pass or use --skip_user_create.", file=sys.stderr)
        raise

    # 2) Create DB + user (if not skipped)
    if not args.skip_user_create:
        create_database_and_user(root_engine, args.db_name, args.db_user, args.db_pass)
        print(f"Ensured database '{args.db_name}' and user '{args.db_user}' exist.")

    # 3) Connect to target DB
    db_engine = make_engine(args.db_user, args.db_pass, args.host, args.port, db=args.db_name, autocommit=False)

    # 4) Build schema
    print("Building schema…")
    build_schema(db_engine)
    print("Schema created.")

    # 5) Load CSVs (parents → children)
    print("Loading CSVs…")
    load_tables(db_engine, args)
    print("CSV load complete.")

    # 6) Run integrity checks
    print("\nIntegrity checks:")
    run_checks(db_engine)
    print("\nAll done ✅")

run_part2_etl(engine, out_dir="etl_outputs")

if __name__ == "__main__":
    main()
