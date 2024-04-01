import pandas as pd


def process_budget_data(path):
    df = read_excel_file(path)
    df = wrangle_dataframe(df)
    return df


def read_excel_file(path):
    df = pd.read_excel(path, sheet_name="Cons", skiprows=2)
    return df


def wrangle_dataframe(df):
    df = rename_columns(df)
    df = select_columns(df)
    df = process_textual_columns(df)
    df = process_numeric_columns(df)
    df = split_period(df)

    # Add columns
    df["Plnt"] = df.apply(add_plant_info, axis="columns")
    df["Account Class"] = df.apply(add_account_class, axis="columns")
    df["RecordType"] = df.apply(add_record_type, axis="columns")

    # Miscellaneous
    df = sales_in_full(df)
    df = remove_na(df)
    df = remove_zero(df)
    df = reorder_columns(df)
    return df


def rename_columns(df):
    df = df.rename(
        columns={
            "Material (local)": "Product",
            "Profit Center": "Profit Ctr",
            "Ext./ICO": "Account Class",
            "Sold-to (central)": "Sold-to party",
            "Sold-to (central) (pivot)": "Sold-to Name 1",
        }
    )
    return df


def select_columns(df):
    col_category = [
        "Account Class",
        "Product",
        "Material Description",
        "Profit Ctr",
        "Sold-to party",
        "Sold-to Name 1",
    ]
    col_vol = [f"2023_{month:02d} vol" for month in range(1, 13)]
    col_amt = [f"2023_{month:02d} k LC" for month in range(1, 13)]

    df = df[col_category + col_vol + col_amt]
    return df


def process_textual_columns(df):
    data_types = {
        "Account Class": str,
        "Product": str,
        "Material Description": str,
        "Profit Ctr": str,
        "Sold-to party": str,
        "Sold-to Name 1": str,
    }
    df = df.astype(data_types)
    return df


def process_numeric_columns(df):
    col_category = [
        "Account Class",
        "Product",
        "Material Description",
        "Profit Ctr",
        "Sold-to party",
        "Sold-to Name 1",
    ]
    col_vol = [f"2023_{month:02d} vol" for month in range(1, 13)]
    col_amt = [f"2023_{month:02d} k LC" for month in range(1, 13)]

    # Split data into volume and sales amount dataframes
    df_vol = df[col_category + col_vol]
    df_amt = df[col_category + col_amt]

    # Pivot both dataframes to tidy format
    df_vol = pd.melt(df_vol, id_vars=col_category, var_name="Period", value_name="Qty")
    df_amt = pd.melt(
        df_amt, id_vars=col_category, var_name="Period", value_name="Sales_k_LC"
    )

    # Combine the two dataframes
    df = pd.concat([df_vol, df_amt["Sales_k_LC"]], axis="columns")

    # Clean up the 'Period' column by removing ' vol' from the values
    df["Period"] = df["Period"].str.replace(" vol", "")

    # Specify data types
    df = df.astype({"Qty": float, "Sales_k_LC": float})

    return df


def split_period(df):
    # Split Period into Year / Month
    df[["Year", "Month"]] = df["Period"].str.split("_", expand=True)
    df[["Year", "Month"]] = df[["Year", "Month"]].astype(float)
    # To match with Actual
    # df["Year"] = df["Year"] + 1
    # Specify data types
    df = df.astype({"Year": int, "Month": int})
    return df


def add_plant_info(row):
    # Assign Plnt based on Profit Ctr
    if row["Profit Ctr"] == "50802-018":
        return "2182"
    else:
        return "0180"


def add_account_class(row):
    # Change Account Class to match with actual data
    if row["Account Class"] == "ICO":
        return "NSI"
    elif row["Account Class"] == "Ext." and "Continental" in row["Sold-to Name 1"]:
        return "NSR"
    else:
        return "NSE"


def add_record_type(row):
    # Add a new column RecordType
    if row["Product"] == "0":
        return "B"
    else:
        return "F"


def sales_in_full(df):
    df["Sales_LC"] = df["Sales_k_LC"] * 1000
    return df


def remove_na(df):
    # Remove NA values in Profit center
    return df.dropna(subset=["Profit Ctr"])


def remove_zero(df):
    # Remove zero values in Qty and Sales
    return df[(df["Qty"] != 0) | (df["Sales_LC"] != 0)]


def reorder_columns(df):
    df = df[
        [
            "Period",
            "Year",
            "Month",
            "Profit Ctr",
            "Plnt",
            "Account Class",
            "RecordType",
            "Product",
            "Material Description",
            "Sold-to party",
            "Sold-to Name 1",
            "Qty",
            "Sales_LC",
        ]
    ]
    return df
