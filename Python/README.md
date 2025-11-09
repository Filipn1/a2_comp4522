# Assignment 2 — Option B (sqlite3)

This folder contains a full-Python pipeline using **SQLite** and **pandas**.

## How to run
Create a virtual environment:
py -3 -m venv .venv
.\.venv\Scripts\activate.bat
Install these packages:
pip install pandas numpy scikit-learn matplotlib jupyter
Run using single line: 
py etl_sqlite.py --in "C:\Users\izzah\OneDrive\Documents\COMP 4522\a2_comp4522\Python" --out "outputs" --db "edu.db" --departments_csv "Department_Information.csv" --students_csv "Student_Counceling_Information.csv" --performance_csv "Student_Performance_Data.csv" --employees_csv "Employee_Information.csv"

- The script loads CSVs from `data/`, writes exception reports & `transformed.csv` to `outputs/`, and creates/refreshes `edu.db`.
- A starter notebook `analysis.ipynb` is provided to explore the transformed dataset and run simple predictions.
