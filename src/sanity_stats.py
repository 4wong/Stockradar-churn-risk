import sqlite3
import pandas as pd

conn = sqlite3.connect("data/processed/stockradar.db")

df = pd.read_sql("""
SELECT is_pro_by_pred, churn_by_90, sms_fail_rate_30d, roi_ratio_30d, integration_errors_30d
FROM merchant_health_features
WHERE is_pro_by_pred = 1
""", conn)

print("Pro merchants only:", len(df))
print("\nAverages by churn label (Pro only):")
print(df.groupby("churn_by_90")[["sms_fail_rate_30d","roi_ratio_30d","integration_errors_30d"]].mean())

print("\nQuantiles (Pro only):")
print(df[["sms_fail_rate_30d","roi_ratio_30d"]].quantile([0.1,0.5,0.9]))

conn.close()
