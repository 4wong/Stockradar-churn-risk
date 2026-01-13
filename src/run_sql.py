import sqlite3
from pathlib import Path

DB_PATH = "data/processed/stockradar.db"
SQL_PATH = "sql/clean_views.sql"

sql = Path(SQL_PATH).read_text(encoding="utf-8", errors="strict")
print("Loaded SQL chars:", len(sql))

conn = sqlite3.connect(DB_PATH)
try:
    conn.executescript(sql)
    conn.commit()
    print("SQL applied successfully.")
finally:
    conn.close()
