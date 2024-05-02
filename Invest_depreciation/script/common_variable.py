import pandas as pd


# Variables

# Monthly update
GPA_version = "v380"
actual_month_end = "2023-11-30"  # 완료된 월 마감

# Yearly update
asset_filename = "2023-11_Asset History Leger_20231130.xlsx"
GPA_filename = "2023-11_GPA_WMS - All data report_FC.xlsx"
spending_total_col = "spend_fc_2023"
current_year = "2023"
current_year_end = pd.to_datetime(current_year + "-12-31")
period_start = "2023-01-31"  # 1월말, 즉 yyyy-01-31
period_end = "2024-01-01"  # 연말 + 1일, 즉 yyyy-01-01
