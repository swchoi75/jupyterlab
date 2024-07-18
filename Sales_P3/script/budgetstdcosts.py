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

    # Read data
    df = pd.read_csv(input_file)
    df_0180 = pd.read_excel(meta_1, sheet_name="MM")
    df_2182 = pd.read_excel(meta_2, sheet_name="MM")

    # Process data
    df_meta = (
        pd.concat([df_0180, df_2182], ignore_index=True)
        .pipe(rename_columns)
        .pipe(clean_names)
        # select columns
        .loc[:, ["product", "profit_ctr", "material_type", "standard_price"]]
    )

    df = df.merge(df_meta, on=["profit_ctr", "product"], how="left").pipe(
        budget_std_costs
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
