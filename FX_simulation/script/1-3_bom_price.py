import pandas as pd
import numpy as np
from pathlib import Path


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Functions
def filter_out_curr(df, currency="CNY"):
    return df[df["cur"] != currency]


def add_ICO_or_NOT(df):
    df["outs_ic"] = np.where(
        (df["component_no"] == "AAA2043950000") & (df["cur"] == "USD"), "IC", "FR"
    )
    return df


def aggregate_data(df):
    df = (
        df.groupby(["year", "product", "cur", "outs_ic"], dropna=False)
        .agg({"total_amount_org_cur_": "sum"})
        .reset_index()
    )
    return df


def main():

    # Filenames
    input_file = path / "data" / "BOM.parquet"
    output_file = path / "data" / "BOM_price.csv"

    # Read data
    df = pd.read_parquet(input_file)

    # Process data
    df = df.pipe(filter_out_curr, "CNY").pipe(add_ICO_or_NOT).pipe(aggregate_data)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created.")


if __name__ == "__main__":
    main()
