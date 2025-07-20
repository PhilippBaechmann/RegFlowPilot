# Pilot: Day 1 Assumptions

- We have a local SQL Server running in Docker.
- We will demo **one** check: PRIIPs Total Expense Ratio (TER) variance.
- Source data fields: ISIN, Valuation Date (month-end), Vendor TER (bps), Mgmt Fee (bps), Tx Cost (bps), Perf Fee (bps).
- Calculated TER (bps) = Mgmt + Tx + Perf.
- Flag if |Vendor TER − Calculated TER| > **5 bps**.
- All data used today is **synthetic demo data** (not Partners Group live data).
