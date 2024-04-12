import pandas as pd
from pathlib import Path

# from src.common_function import add_responsibilities
from common_function import add_responsibilities


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Functions
def read_GPA(filename):
    df = pd.read_csv(
        filename,
        dtype={
            "investment_type": str,
            "financial_statement_item": str,
            "input_cost_center": str,
        },
    )
    return df


def read_SAP(filename):
    df = pd.read_csv(
        filename,
        dtype={
            "asset_class": str,
            "cost_center": str,
            "asset_no": str,
            "sub_no": str,
        },
    )
    return df


def main():

    # Filenames
    input_1 = path / "output" / "4_fc_depreciation_future_assets.csv"
    input_2 = path / "output" / "4_fc_depreciation_existing_assets.csv"
    input_3 = path / "output" / "4_fc_depreciation_asset_under_construction.csv"
    output_file = path / "output" / "4_fc_depreciation_merged.csv"

    # Read data
    df_1 = read_GPA(input_1)
    df_2 = read_SAP(input_2)
    df_3 = read_SAP(input_3)

    # Add source column
    df_1["source"] = "GPA"
    df_2["source"] = "SAP"
    df_3["source"] = "SAP_AUC"

    # Add new column for SAP data
    df_2["asset_category"] = "existing"
    df_3["asset_category"] = "Auc"

    # Combine three dataframes
    df = pd.concat([df_1, df_2, df_3])

    # Businss logic: Add a new column
    df["responsibilities"] = df.apply(add_responsibilities, axis="columns")

    # Reorder columns
    df = df[
        ["source", "responsibilities"]
        + [col for col in df.columns if col not in ["source", "responsibilities"]]
    ]

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
