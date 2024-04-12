import pandas as pd


# Variables

# Monthly update
GPA_version = "v001"
actual_month_end = "2024-02-29"

# Yearly update
asset_filename = "2024_Asset History Ledger.XLS"
GPA_filename = "2024_GPA_WMS - All data report_FC.xlsx"
spending_total_col = "spend_fc_2024"
current_year = "2024"
current_year_end = pd.to_datetime(current_year + "-12-31")
period_start = "2024-01-31"
period_end = "2025-01-01"
