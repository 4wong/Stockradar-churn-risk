import sqlite3
import pandas as pd

conn = sqlite3.connect("data/processed/stockradar.db")

print(pd.read_sql("""
SELECT COUNT(*) AS n_rows,
       SUM(churn_by_90) AS n_churn,
       AVG(churn_by_90) AS churn_rate
FROM merchant_health_features;
""", conn))

print("\nPro vs churn:")
print(pd.read_sql("""
SELECT is_pro_by_pred, churn_by_90, COUNT(*) AS n
FROM merchant_health_features
GROUP BY 1,2
ORDER BY 1,2;
""", conn))

print("\nSample rows:")
print(pd.read_sql("""
SELECT merchant_id, is_pro_by_pred, sms_fail_rate_30d, roi_ratio_30d, churn_by_90
FROM merchant_health_features
ORDER BY RANDOM()
LIMIT 10;
""", conn))

conn.close()
