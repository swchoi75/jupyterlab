import pandas as pd


# Variables

# Monthly update
version = "Plan"  # choose between "FC" or "Plan"
actual_month_end = "2024-05-31"  # 완료된 월 마감
GPA_version_1 = "v001"
GPA_version_2 = "v388"  # optional version for Next Year Budget (frozen)

# Yearly update

# Text manipulation
spending_total_col = "spend_fc_2024"
text_to_remove_1 = "spend_fc_"
text_to_remove_2 = "spend_plan"  # optional text for Next Year Budget (frozen)

if version == "FC":
    # Filenames
    asset_filename = "2024_Asset History Ledger_FC.XLS"
    GPA_filename = "2024_GPA_WMS - All data report_FC.xlsx"
    result_filename = "fc_depreciation_this_year.csv"

    # Reclassify to AuC
    current_year = "2024"
    current_year_end = pd.to_datetime(current_year + "-12-31")
    # Depreciation period
    period_start = "2024-01-01"  # 1월초, 즉 yyyy-01-01
    period_end = "2025-01-01"  # 연말 + 1일, 즉 yyyy-01-01

elif version == "Plan":
    # Filenames
    asset_filename = "2024_Asset History Ledger_Plan.XLS"
    GPA_filename = "2024_GPA_WMS - All data report_Plan.xlsx"
    # GPA_filename = "2024_GPA_WMS - All data report_Plan_frozen.xlsx"
    result_filename = "fc_depreciation_next_year.csv"
    # Reclassify to AuC
    current_year = "2025"
    current_year_end = pd.to_datetime(current_year + "-12-31")
    # Depreciation period
    period_start = "2025-01-01"  # 1월초, 즉 yyyy-01-01
    period_end = "2026-01-01"  # 연말 + 1일, 즉 yyyy-01-01
