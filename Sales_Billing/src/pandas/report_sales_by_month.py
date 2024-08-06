import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def rename_columns(df):
    df.columns = [
        "prctr",
        "product_hier",
        "customer",
        "material",
        "description",
        "platform",
        "division",
        "customer_part_no",
        "material_type",
        "std_cost",
        "01_qty",
        "01_sum",
        "02_qty",
        "02_sum",
        "03_qty",
        "03_sum",
        "04_qty",
        "04_sum",
        "05_qty",
        "05_sum",
        "06_qty",
        "06_sum",
        "07_qty",
        "07_sum",
        "08_qty",
        "08_sum",
        "09_qty",
        "09_sum",
        "10_qty",
        "10_sum",
        "11_qty",
        "11_sum",
        "12_qty",
        "12_sum",
        "total_qty",
        "total_sum",
    ]

    return df


def overwrite_with_blank_data(df):
    df["std_cost"] = ""  # Make std_cost into blank
    return df


def remove_missing_values(df):
    df = df.dropna(subset=["prctr"])
    return df


def change_data_type(df):
    # change data type from float to integer
    float_cols = df.select_dtypes(include=["float"]).columns
    df[float_cols] = df[float_cols].astype(int)
    return df


def main():

    # Filenames
    input_file = path / "data" / "Sales_20240702.xls"
    output_file = path / "output" / "Sales by month.csv"

    # Read data
    df = pd.read_csv(input_file, delimiter="\t", encoding="latin1")

    # Process data
    df = (
        df.pipe(rename_columns)
        .pipe(overwrite_with_blank_data)
        .pipe(remove_missing_values)
        .pipe(change_data_type)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
