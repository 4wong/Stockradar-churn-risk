import sqlite3
from pathlib import Path

DB_PATH = "data/processed/stockradar.db"
SQL_PATH = "sql/build_features.sql"

conn = sqlite3.connect(DB_PATH)
conn.executescript(Path(SQL_PATH).read_text(encoding="utf-8"))
conn.commit()
conn.close()

print("Built merchant_health_features.")
