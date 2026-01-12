"""
StockRadar: Representative Data Engine
--------------------------------------
Generates a 'dirty' production-like dataset for churn analysis.
Simulates the 60–90 day retention cliff and correlates churn with SMS failure rates and merchant ROI.

Output:
- data/raw/*.csv
- data/processed/stockradar.db
"""

import os
import sqlite3
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Optional, List, Tuple, Dict

import numpy as np
import pandas as pd

RANDOM_SEED = 42
N_MERCHANTS = 864

START_DATE = date(2025, 3, 1)
END_DATE = date(2026, 1, 1)

PRO_PRICE = 29.0
CLIFF_START, CLIFF_END = 60, 90
LABEL_HORIZON = 90


def seed_all(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + np.exp(-x))


def seasonal_multiplier(d: date) -> float:
    if d.month in (11, 12):
        return 1.4
    if d.month == 1:
        return 0.8
    return 1.0


def dirty_plan_code() -> str:
    return random.choices(
        ["pro", "PRO", "Pro Tier", "pro_plan_v2", "Pro", "PRO_TIER"],
        weights=[0.35, 0.10, 0.15, 0.15, 0.15, 0.10],
    )[0]


def dirty_event_type(base: str) -> str:
    variants = {
        "dashboard_view": ["dashboard_view", "Dashboard_View", "DASHBOARD_VIEW"],
        "sms_sent": ["sms_sent", "SMS_SENT", "smsSent"],
        "sms_failed": ["sms_failed", "SMS_FAILED", "smsFail"],
        "integration_error": ["integration_error", "INTEGRATION_ERROR", "integrationError"],
    }
    return random.choice(variants.get(base, [base]))


def sample_cancel_day_if_churn() -> int:
    if random.random() < 0.80:
        return random.randint(CLIFF_START, CLIFF_END)
    return random.randint(15, 59)


@dataclass
class MerchantLatents:
    tech_risk: float
    engagement: float
    roi_strength: float


def generate_latents() -> MerchantLatents:
    return MerchantLatents(
        tech_risk=float(np.random.beta(2.0, 8.0)),
        engagement=float(np.random.beta(3.0, 3.0)),
        roi_strength=float(np.random.beta(2.5, 4.5)),
    )


def churn_probability(lat: MerchantLatents, roi_ratio: float, fail_rate: float) -> float:
    roi_term = -2.2 * (roi_ratio - 1.0)
    fail_term = 3.0 * (fail_rate - 0.08)
    tech_term = 1.2 * (lat.tech_risk - 0.25)
    engage_term = -1.0 * (lat.engagement - 0.5)
    logit = -0.35 + roi_term + fail_term + tech_term + engage_term
    return float(clamp(sigmoid(logit), 0.02, 0.95))


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS dim_merchants;
CREATE TABLE dim_merchants (
    merchant_id INTEGER PRIMARY KEY,
    install_date DATE NOT NULL,
    country TEXT,
    acquisition_channel TEXT,
    industry TEXT,
    is_active INTEGER DEFAULT 1
);

DROP TABLE IF EXISTS fact_subscription_events;
CREATE TABLE fact_subscription_events (
    event_id INTEGER PRIMARY KEY,
    merchant_id INTEGER NOT NULL,
    event_timestamp DATETIME NOT NULL,
    event_type TEXT NOT NULL,
    monthly_price REAL,
    plan_code_raw TEXT,
    FOREIGN KEY (merchant_id) REFERENCES dim_merchants(merchant_id)
);

DROP TABLE IF EXISTS fact_app_events;
CREATE TABLE fact_app_events (
    event_id INTEGER PRIMARY KEY,
    merchant_id INTEGER NOT NULL,
    event_timestamp DATETIME NOT NULL,
    event_type TEXT NOT NULL,
    metadata TEXT,
    FOREIGN KEY (merchant_id) REFERENCES dim_merchants(merchant_id)
);

DROP TABLE IF EXISTS fact_recovered_sales_daily;
CREATE TABLE fact_recovered_sales_daily (
    id INTEGER PRIMARY KEY,
    merchant_id INTEGER NOT NULL,
    event_date DATE NOT NULL,
    recovered_sales_nzd REAL NOT NULL DEFAULT 0.0,
    FOREIGN KEY (merchant_id) REFERENCES dim_merchants(merchant_id)
);

CREATE INDEX IF NOT EXISTS idx_sub_merchant_time ON fact_subscription_events(merchant_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_app_merchant_time ON fact_app_events(merchant_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_rev_merchant_date ON fact_recovered_sales_daily(merchant_id, event_date);
"""


def generate_app_events(
    m_id: int,
    install_dt: date,
    end_dt: date,
    lat: MerchantLatents,
) -> Tuple[List[Tuple[int, str, str, Optional[str]]], Dict[str, float]]:
    events: List[Tuple[int, str, str, Optional[str]]] = []
    sms_sent = 0
    sms_failed = 0

    base_dash = 0.20 + 0.90 * lat.engagement
    base_sms = 0.08 + 0.25 * lat.engagement
    base_err = 0.01 + 0.20 * lat.tech_risk

    for day_offset in range((end_dt - install_dt).days):
        d = install_dt + timedelta(days=day_offset)
        mult = seasonal_multiplier(d)

        if random.random() < base_dash * mult:
            ts = datetime.combine(d, datetime.min.time()) + timedelta(
                hours=random.randint(7, 22), minutes=random.randint(0, 59)
            )
            md = None if random.random() < 0.03 else '{"page":"overview"}'
            events.append((m_id, ts.isoformat(sep=" "), dirty_event_type("dashboard_view"), md))

        if random.random() < base_err * mult:
            ts = datetime.combine(d, datetime.min.time()) + timedelta(
                hours=random.randint(0, 23), minutes=random.randint(0, 59)
            )
            md = None if random.random() < 0.03 else '{"error":"webhook_timeout"}'
            events.append((m_id, ts.isoformat(sep=" "), dirty_event_type("integration_error"), md))

        if random.random() < base_sms * mult:
            sms_sent += 1
            fail_p = clamp(0.03 + 0.45 * lat.tech_risk, 0.01, 0.60)
            is_fail = random.random() < fail_p
            ts = datetime.combine(d, datetime.min.time()) + timedelta(
                hours=random.randint(8, 23), minutes=random.randint(0, 59)
            )
            if is_fail:
                sms_failed += 1
                md = None if random.random() < 0.03 else '{"reason":"carrier_reject"}'
                events.append((m_id, ts.isoformat(sep=" "), dirty_event_type("sms_failed"), md))
            else:
                md = None if random.random() < 0.03 else '{"provider":"twilio"}'
                events.append((m_id, ts.isoformat(sep=" "), dirty_event_type("sms_sent"), md))

        if events and random.random() < 0.002:
            events.append(events[-1])

    stats = {"fail_rate": float(sms_failed / sms_sent) if sms_sent > 0 else 0.0}
    return events, stats


def generate_sales(m_id: int, install_dt: date, end_dt: date, lat: MerchantLatents) -> List[Tuple[int, str, float]]:
    rows: List[Tuple[int, str, float]] = []
    for day_offset in range((end_dt - install_dt).days):
        d = install_dt + timedelta(days=day_offset)
        if random.random() < 0.06:
            continue
        base = 12.0 * (0.2 + 1.6 * lat.engagement) * (0.2 + 1.8 * lat.roi_strength) * seasonal_multiplier(d)
        val = clamp(base * float(np.random.lognormal(0, 0.6)), 0.0, 900.0)
        if random.random() < (0.25 - 0.15 * lat.engagement):
            val *= 0.05
        rows.append((m_id, d.isoformat(), round(float(val), 2)))
    return rows


def main() -> None:
    seed_all(RANDOM_SEED)
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)

    m_rows: List[Tuple] = []
    sub_rows: List[Tuple] = []
    app_rows: List[Tuple] = []
    rev_rows: List[Tuple] = []

    sub_id, app_id, rev_id = 1, 1, 1

    countries = ["NZ", "AU", "US", "GB", "CA"]
    channels = ["twitter", "discord", "referral", "organic", "unknown"]
    industries = ["Apparel", "Beauty", "Tech", "HomeGoods", "Sports", None]

    for m_id in range(1, N_MERCHANTS + 1):
        install_dt = random_date(START_DATE, END_DATE)
        lat = generate_latents()

        m_rows.append(
            (
                m_id,
                install_dt.isoformat(),
                random.choice(countries),
                random.choices(channels, weights=[0.25, 0.20, 0.15, 0.25, 0.15])[0],
                random.choice(industries),
                1,
            )
        )

        trial_ts = datetime.combine(install_dt, datetime.min.time()) + timedelta(hours=9)
        sub_rows.append((sub_id, m_id, trial_ts.isoformat(sep=" "), "trial_start", 0.0, "free"))
        sub_id += 1

        adopts_pro = random.random() < 0.45

        if adopts_pro:
            upgrade_dt = install_dt + timedelta(days=random.randint(7, 21))
            upgrade_ts = datetime.combine(upgrade_dt, datetime.min.time()) + timedelta(hours=10)
            sub_rows.append((sub_id, m_id, upgrade_ts.isoformat(sep=" "), "upgrade_pro", PRO_PRICE, dirty_plan_code()))
            sub_id += 1

            window_end = install_dt + timedelta(days=LABEL_HORIZON)
            m_events, stats = generate_app_events(m_id, install_dt, window_end, lat)
            m_sales = generate_sales(m_id, install_dt, window_end, lat)

            roi = (sum(x[2] for x in m_sales) / (3 * PRO_PRICE)) if (3 * PRO_PRICE) > 0 else 0.0
            churns = random.random() < churn_probability(lat, float(roi), float(stats["fail_rate"]))

            cancel_ts: Optional[datetime] = None
            cancel_dt: Optional[date] = None

            if churns:
                cancel_day = sample_cancel_day_if_churn()
                cancel_dt = install_dt + timedelta(days=cancel_day)
                cancel_ts = datetime.combine(cancel_dt, datetime.min.time()) + timedelta(hours=12)
                sub_rows.append((sub_id, m_id, cancel_ts.isoformat(sep=" "), "cancel", PRO_PRICE, dirty_plan_code()))
                sub_id += 1

                m_events = [e for e in m_events if datetime.fromisoformat(e[1]) < cancel_ts]
                m_sales = [s for s in m_sales if date.fromisoformat(s[1]) < cancel_dt]

            for e in m_events:
                app_rows.append((app_id, *e))
                app_id += 1

            for s in m_sales:
                rev_rows.append((rev_id, *s))
                rev_id += 1

    df_m = pd.DataFrame(m_rows, columns=["merchant_id", "install_date", "country", "acquisition_channel", "industry", "is_active"])
    df_s = pd.DataFrame(sub_rows, columns=["event_id", "merchant_id", "event_timestamp", "event_type", "monthly_price", "plan_code_raw"])
    df_a = pd.DataFrame(app_rows, columns=["event_id", "merchant_id", "event_timestamp", "event_type", "metadata"])
    df_r = pd.DataFrame(rev_rows, columns=["id", "merchant_id", "event_date", "recovered_sales_nzd"])

    conn = sqlite3.connect("data/processed/stockradar.db")
    conn.executescript(SCHEMA_SQL)

    df_m.to_sql("dim_merchants", conn, index=False, if_exists="append")
    df_s.to_sql("fact_subscription_events", conn, index=False, if_exists="append")
    df_a.to_sql("fact_app_events", conn, index=False, if_exists="append")
    df_r.to_sql("fact_recovered_sales_daily", conn, index=False, if_exists="append")
    conn.close()

    df_m.to_csv("data/raw/dim_merchants.csv", index=False)
    df_s.to_csv("data/raw/fact_subscription_events.csv", index=False)
    df_a.to_csv("data/raw/fact_app_events.csv", index=False)
    df_r.to_csv("data/raw/fact_recovered_sales_daily.csv", index=False)

    print(f"Success! Generated data for {N_MERCHANTS} merchants.")
    

if __name__ == "__main__":
    main()
