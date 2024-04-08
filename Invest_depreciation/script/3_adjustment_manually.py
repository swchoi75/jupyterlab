import pandas as pd
from pathlib import Path


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Functions
def read_data(filename):
    df = pd.read_csv(
        filename,
        dtype={
            "investment_type": str,
            "financial_statement_item": str,
            "input_cost_center": str,
        },
    )
    return df


def filter_by_val(df, values):
    filter = df["sub"].isin(values)
    df_filtered = df[filter].reset_index().drop(columns="index")
    df_remaining = df[~filter].reset_index().drop(columns="index")
    return df_filtered, df_remaining


def supplier_tooling(df):
    """Manual adjustment"""
    df["category_of_investment"] = "8"
    df["category_description"] = "Tooling located at supplier"
    df["financial_statement_item"] = "122637000"
    df["fs_item_description"] = "Molds / containers / tooling"
    df["gl_account"] = "K432"
    df["gl_account_description"] = "Depreciation special tools - contr spplr"
    df["fix_var"] = "var"
    df["mv_type"] = "211 Depr. of tools"
    df["useful_life_year"] = 4  # important change
    return df


def main():
    # Filenames
    input_file = path / "output" / "2_fc_acquisition_future_assets.csv"
    output_file = path / "output" / "3_fc_acquisition_future_assets_adj.csv"

    # Read data
    df = read_data(input_file)

    # Split data
    df_filtered, df_remaining = filter_by_val(
        df, ["IF310241"]
    )  # HQ created sub master, and we cannot change category of investment

    # Manual adjustment as supplier tooling
    df_filtered = supplier_tooling(df_filtered)

    # Concatenate dataframe
    df = pd.concat([df_filtered, df_remaining])

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
