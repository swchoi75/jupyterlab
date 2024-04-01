import pandas as pd
import numpy as np


def process_actual_data(path):
    # Read CSV file as string
    df = pd.read_csv(path, dtype="str")

    # Specify data types
    df = df.astype(
        {
            "Quantity": float,
            "TotSalesLC": float,
            "Stock val.": float,
        }
    )

    # Perform data wrangling operations on the dataframe
    df = wrangle_dataframe(df)

    # Return the processed dataframe
    return df


def wrangle_dataframe(df):
    df = group_summarize(df)
    df = rename_columns(df)
    df = filter_out_zero(df)
    df = split_period(df)
    return df


def group_summarize(df):
    df = (
        df.groupby(
            [
                "Period",
                "Profit Ctr",
                "RecordType",
                "Cost Elem.",
                "Account Class",
                "Plnt",
                "Product",
                "MatNr Descr.",
                "Sold-to party",
                "Sold-to Name 1",
            ],
            dropna=False,  # Important, as we have missing values in Plnt, Product, etc
        )
        .agg({"Quantity": "sum", "TotSalesLC": "sum", "Stock val.": "sum"})
        .reset_index()
    )
    return df


def rename_columns(df):
    new_columns = {
        "Quantity": "Qty",
        "TotSalesLC": "Sales_LC",
        "Stock val.": "STD_Costs",
        "MatNr Descr.": "Material Description",
    }
    return df.rename(columns=new_columns)


def filter_out_zero(df):
    # Select rows where quantity, sales, or standard costs are non-zero
    non_zero_rows = ~((df["Qty"] == 0) & (df["Sales_LC"] == 0) & (df["STD_Costs"] == 0))

    # Filter out zero rows and return the resulting dataframe
    filtered_df = df[non_zero_rows]
    return filtered_df


def split_period(df):
    # Split period into year and month columns
    df[["Year", "Month"]] = df["Period"].str.split(".", expand=True)
    df[["Year", "Month"]] = df[["Year", "Month"]].astype(float)
    # To match with budget
    df["Year"] = df["Year"] - 1
    # Specify data types
    df = df.astype({"Year": int, "Month": int})
    return df


def kappa_hev(path):
    df = pd.read_excel(path, sheet_name="format")
    df["STD_Costs"] = np.where(df["D/C"] == 40, df["STD_Costs"], df["STD_Costs"] * -1)
    df = df.drop("D/C", axis="columns")
    return df
