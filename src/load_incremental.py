"""
load_incremental.py
────────────────────────────────────────────────────────────
▪  Adds one synthetic month of TER data for every fund
▪  Appends to FactCosts, then triggers the QC stored‑proc
"""

import random
from datetime import date
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ── 1.  SQL‑Server connection ─────────────────────────────
RAW_PW = "StrongP@ssw0rd!"                      # ← your real sa password
PW      = quote_plus(RAW_PW)                    # URL‑encode specials

CONN_STR = (
    f"mssql+pyodbc://sa:{PW}@localhost:1433/RegFlow?"
    "driver=ODBC+Driver+18+for+SQL+Server"
    "&Encrypt=no&TrustServerCertificate=yes"
)

ENG = create_engine(CONN_STR, fast_executemany=True)

# ── 2.  Helper: build one month’s fake TER split ──────────
def fake_month(_period: pd.Period):
    mgmt  = round(random.uniform(1.00, 1.40), 2)
    tx    = round(random.uniform(0.10, 0.30), 2)
    perf  = round(random.uniform(0.00, 0.50), 2)
    vendor = round(mgmt + tx + perf, 2)
    return vendor, mgmt, tx, perf

# ── 3.  Main loader routine ───────────────────────────────
def load_today():
    month = pd.Period(date.today(), freq="M")           # e.g. 2025‑07

    funds = pd.read_sql("SELECT FundID, ISIN FROM DimFund", ENG)

    rows = []
    for _, row in funds.iterrows():
        vendor, mgmt, tx, perf = fake_month(month)
        rows.append(
            dict(
                FundID       = row.FundID,
                ValDate      = month.to_timestamp(),
                VendorTER_bp = vendor,
                CalcTER_bp   = vendor,
                MgmtFee_bp   = mgmt,
                TxCost_bp    = tx,
                PerfFee_bp   = perf,
            )
        )

    pd.DataFrame(rows).to_sql(
        "FactCosts", ENG, if_exists="append", index=False
    )

    # run QC proc (SQLAlchemy 2 style)
    with ENG.begin() as conn:
        conn.execute(
            text("EXEC dbo.sp_QC_CheckTER @Threshold=:thr"),
            {"thr": 5.0}                # 5 bps threshold
        )

    print(f"{len(rows)} rows inserted for {month} and QC executed.")

# ── 4.  Entrypoint ────────────────────────────────────────
if __name__ == "__main__":
    load_today()
