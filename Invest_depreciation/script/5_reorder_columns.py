import pandas as pd
from pathlib import Path


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


def main():
    # Filenames
    input_file = path / "output" / "5_fc_depreciation_cf_allocation.csv"
    output_file = path / "output" / "fc_depreciation.csv"

    # Read data
    df = pd.read_csv(input_file, dtype=str)  # no calculation is needed

    # Reorder columns
    column_orders = [
        "source",
        "responsibilities",
        # GPA
        "location_sender",
        "outlet_sender",
        "category_of_investment",
        "category_of_invest_historic",
        "investment_type",
        "status",
        "master",
        "master_description",
        "sub",
        "sub_description",
        "basic_or_project",
        # SAP
        "wbs_element",
        "asset_class",
        "asset_no",
        "sub_no",
        "asset_description",
        "useful_life_month",
        "asset_class_name",
        # meta for GPA and SAP
        "category_description",
        "financial_statement_item",
        "fs_item_description",
        "fs_item_sub",
        "mv_type",
        "gl_account",
        "gl_account_description",
        "zv2_account",
        "fix_var",
        "cost_center",
        "cc_validity",
        "profit_center",
        # Manual input
        "input_gl_account",
        "input_cost_center",
        "PPAP",
        "input_useful_life_year",
        # CF Allocation
        "rec_prctr",
        "percentage",
        "cu_no",
        "plant_no",
        "outlet_no",
        "bu_receiver",
        "division_receiver",
        "location_receiver",
        "outlet_receiver",
        # Value columns to follow
    ]

    # Reorder columns
    df = df[column_orders + [col for col in df.columns if col not in column_orders]]

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
