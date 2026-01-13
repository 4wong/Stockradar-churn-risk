import sqlite3, pandas as pd
conn = sqlite3.connect("data/processed/stockradar.db")

print(pd.read_sql("""
SELECT event_type, COUNT(*) AS n
FROM v_app_events_clean
GROUP BY 1
ORDER BY n DESC;
""", conn))

print(pd.read_sql("""
SELECT plan_tier, event_type, COUNT(*) AS n
FROM v_sub_events_clean
GROUP BY 1,2
ORDER BY n DESC;
""", conn))

conn.close()
