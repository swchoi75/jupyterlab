import pandas as pd


# Variables

# Monthly update
actual_month_end = "2023-11-30"
asset_filename = "2023-11_Asset History Leger_20231130.xlsx"
GPA_filename = "2023-11_GPA_WMS - All data report_FC.xlsx"
GPA_version = "v380"

# Yearly update
spending_total_col = "spend_fc_2023"
current_year = "2023"
current_year_end = pd.to_datetime(current_year + "-12-31")
period_start = "2023-01-31"
period_end = "2024-01-01"
