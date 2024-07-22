import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def rename_columns(df):
    df = df.rename(
        columns={
            "Product Hierachy": "product_hierarchy",
            "Material": "product",
            "Profit Center": "profit_ctr",
        }
    )
    return df


def select_columns(df, list_of_cols):
    df = df[list_of_cols]
    return df


def budget_std_costs(df):

    # Calculate standard costs
    df["std_costs"] = df["qty"] * df["standard_price"]

    # Specify the data types
    df["std_costs"] = df["std_costs"].astype(float).fillna(0)

    # Drop the standard price column
    df = df.drop(columns=["standard_price"])

    return df


def main():

    # Filenames
    input_file = path / "output" / "2_budget_sales.csv"
    meta_1 = path / "meta" / "Material master_0180.xlsx"
    meta_2 = path / "meta" / "Material master_2182.xlsx"
    output_file = path / "output" / "3_budget_std_costs.csv"
    output_meta = path / "meta" / "material_master.csv"

    # Read data
    df = pd.read_csv(input_file)
    df_0180 = pd.read_excel(meta_1, sheet_name="MM")
    df_2182 = pd.read_excel(meta_2, sheet_name="MM")

    # Process data
    df_meta = (
        pd.concat([df_0180, df_2182], ignore_index=True)
        .pipe(rename_columns)
        .pipe(clean_names)
        .pipe(
            select_columns,
            [
                "product",
                "profit_ctr",
                "material_type",
                "product_hierarchy",
                "standard_price",
            ],
        )
    )

    df_sub = df_meta.drop(columns=["product_hierarchy"])
    df = df.merge(df_sub, on=["profit_ctr", "product"], how="left").pipe(
        budget_std_costs
    )

    # Write data
    df.to_csv(output_file, index=False)
    df_meta.to_csv(output_meta, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
