import pandas as pd
import numpy as np
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def drop_columns(df):
    df = df.drop(
        columns=[
            "doc_no",
            "created_on",
            "created_by",
            "ref_doc_no",
            "rf_itm",
            "ac_documentno",
            "delivery",
            "item",
            "tr_prt",
        ]
    )
    return df


def group_summarize(df):
    df = (
        df.groupby(
            [
                col
                for col in df.columns
                if col not in ["quantity", "totsaleslc", "stock_value"]
            ],
            dropna=False,  # Important, as we have missing values in Plnt, Product, etc
        )
        .agg({"quantity": "sum", "totsaleslc": "sum", "stock_value": "sum"})
        .reset_index()
    )
    return df


def rename_columns(df):
    new_columns = {
        "quantity": "qty",
        "totsaleslc": "sales_lc",
        "stock_value": "std_costs",
        "matnr_descr": "material_description",
    }
    return df.rename(columns=new_columns)


def filter_out_zero(df):
    # Select rows where quantity, sales, or standard costs are non-zero
    non_zero_rows = ~((df["qty"] == 0) & (df["sales_lc"] == 0) & (df["std_costs"] == 0))

    # Filter out zero rows and return the resulting dataframe
    filtered_df = df[non_zero_rows]
    return filtered_df


def split_period(df):
    # Split period into year and month columns
    df[["year", "month"]] = df["period"].str.split(".", expand=True)
    df[["year", "month"]] = df[["year", "month"]].astype(float)
    # To match with budget
    df["year"] = df["year"] - 1
    # Specify data types
    df = df.astype({"year": int, "month": int})
    return df


def clean_column_names(df):
    df = df.clean_names()
    df.columns = df.columns.str.strip("_")
    return df


def process_std_costs(df):
    df["std_costs"] = np.where(df["d_c"] == 40, df["std_costs"], df["std_costs"] * -1)
    df = df.drop("d_c", axis="columns")
    return df


def main():
    # Variable
    year = 2024

    # Filenames
    input_main = path / "db" / f"COPA_Sales_{year}.parquet"
    input_sub = path / "data" / "Actual" / "Kappa HEV adj_costs.xlsx"
    output_file = path / "output" / "1_actual_sales.csv"

    # Read data
    df = pd.read_parquet(input_main)
    df_sub = pd.read_excel(input_sub, sheet_name="format")

    # Process data
    df = (
        df.pipe(drop_columns)
        .pipe(group_summarize)
        .pipe(rename_columns)
        .pipe(filter_out_zero)
        .pipe(split_period)
    )

    kappa_cost = df_sub.pipe(clean_column_names).pipe(process_std_costs)

    df = pd.concat([df, kappa_cost], ignore_index=True)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
