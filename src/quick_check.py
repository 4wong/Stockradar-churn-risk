import sqlite3
import pandas as pd

conn = sqlite3.connect("data/processed/stockradar.db")

for t in ["dim_merchants", "fact_subscription_events", "fact_app_events", "fact_recovered_sales_daily"]:
    n = pd.read_sql(f"SELECT COUNT(*) AS n FROM {t}", conn)["n"][0]
    print(t, n)

print("\nTop subscription event types:")
print(pd.read_sql("""
SELECT lower(event_type) AS event_type, COUNT(*) AS n
FROM fact_subscription_events
GROUP BY 1
ORDER BY n DESC
LIMIT 10;
""", conn))

print("\nTop app event types:")
print(pd.read_sql("""
SELECT lower(event_type) AS event_type, COUNT(*) AS n
FROM fact_app_events
GROUP BY 1
ORDER BY n DESC
LIMIT 10;
""", conn))

conn.close()
