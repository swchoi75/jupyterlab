import pandas as pd


# Variables

# Monthly update
version = "Plan"  # "FC" or "Plan"
GPA_version = "v380"
actual_month_end = "2023-11-30"  # 완료된 월 마감

# Yearly update
if version == "FC":
    # Filenames
    asset_filename = "2023-11_Asset History Leger_20231130.xlsx"
    GPA_filename = "2023-11_GPA_WMS - All data report_FC.xlsx"
    result_filename = "fc_depreciation_this_year.csv"
    # Text manipulation
    spending_total_col = "spend_fc_2023"
    text_to_remove = "spend_fc_"
    # Reclassify to AuC
    current_year = "2023"
    current_year_end = pd.to_datetime(current_year + "-12-31")
    # Depreciation period
    period_start = "2023-01-01"  # 1월초, 즉 yyyy-01-01
    period_end = "2024-01-01"  # 연말 + 1일, 즉 yyyy-01-01

elif version == "Plan":
    # Filenames
    asset_filename = "2023-11_Asset History Leger_20231130.xlsx"
    GPA_filename = "2023-11_GPA_WMS - All data report_Plan.xlsx"
    result_filename = "fc_depreciation_next_year.csv"
    # Text manipulation
    spending_total_col = "spend_fc_2023"
    text_to_remove = "spend_fc_"
    # Reclassify to AuC
    current_year = "2024"
    current_year_end = pd.to_datetime(current_year + "-12-31")
    # Depreciation period
    period_start = "2024-01-01"  # 1월초, 즉 yyyy-01-01
    period_end = "2025-01-01"  # 연말 + 1일, 즉 yyyy-01-01
