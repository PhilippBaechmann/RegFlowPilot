"""
load_fake_data.py  –  Day‑1 demo loader with debug prints
────────────────────────────────────────────────────────────
Generates 10 demo funds × 24 months of TER data and loads
them into the SQL Server database 'RegFlow' (Docker).

Includes DEBUG prints so we can see where rows might vanish.
Remove those prints after counts look correct.
"""

import random
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# ── 1.  Connection details ───────────────────────────────
RAW_PASSWORD = "StrongP@ssw0rd!"          #  ← your real sa password
ENC_PW = quote_plus(RAW_PASSWORD)         #  url‑encode for URI

CONN_STR = (
    f"mssql+pyodbc://sa:{ENC_PW}@localhost:1433/RegFlow?"
    "driver=ODBC+Driver+18+for+SQL+Server"
    "&Encrypt=no"
    "&TrustServerCertificate=yes"
)

# ── 2.  Fake‑data parameters ─────────────────────────────
fake = Faker()
N_FUNDS = 10
MONTHS = pd.period_range("2023-01", periods=24, freq="M")

# ── 3.  Build DataFrames ─────────────────────────────────
def build_frames():
    funds, costs = [], []
    for n in range(N_FUNDS):
        isin = f"CH{10000000+n:08d}"
        funds.append(
            dict(ISIN=isin,
                 FundName=fake.company(),
                 InceptionDate="2022-01-15")
        )
        for m in MONTHS:
            mgmt = round(random.uniform(1.00, 1.40), 2)
            tx   = round(random.uniform(0.10, 0.30), 2)
            perf = round(random.uniform(0.00, 0.50), 2)
            vendor = round(mgmt + tx + perf, 2)
            costs.append(
                dict(
                    ISIN=isin,
                    ValDate=m.to_timestamp(),
                    VendorTER_bp=vendor,
                    CalcTER_bp=vendor,
                    MgmtFee_bp=mgmt,
                    TxCost_bp=tx,
                    PerfFee_bp=perf,
                )
            )
    # inject two anomalies
    costs[5]["VendorTER_bp"]  += 0.20
    costs[40]["VendorTER_bp"] -= 0.25
    return pd.DataFrame(funds), pd.DataFrame(costs)

# ── 4.  Main loader with DEBUG prints ────────────────────
def main():
    df_funds, df_costs = build_frames()

    # DEBUG before any SQL I/O
    print("DEBUG 1  df_funds rows BEFORE SQL  :", len(df_funds))
    print("DEBUG 2  df_costs rows BEFORE SQL  :", len(df_costs))

    engine = create_engine(CONN_STR, fast_executemany=True)

    # upload funds master
    df_funds.to_sql("DimFund", engine, if_exists="append", index=False)

    # retrieve IDs created by SQL Server
    dim = pd.read_sql("SELECT FundID, ISIN FROM DimFund", engine)
    dim["ISIN"] = dim["ISIN"].str.strip()
    print("DEBUG 3  rows now in DimFund table :", len(dim))

    # attach FundID to cost rows
    df_costs = df_costs.merge(dim, on="ISIN").drop(columns=["ISIN"])

    print("DEBUG 4  df_costs rows AFTER merge :", len(df_costs))

    # upload cost facts
    df_costs.to_sql("FactCosts", engine, if_exists="append", index=False)
    print(f"Loaded {len(df_costs)} cost rows into FactCosts.")

if __name__ == "__main__":
    main()



