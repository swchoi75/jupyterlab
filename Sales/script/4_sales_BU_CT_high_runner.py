import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions
def aggregate_data(df):
    df = (
        df.groupby(
            [
                "fy",
                "recordtype",
                "product_hierarchy",
                "PH_description",
                "customer_engines",
                "customer_products",
                "product",
                "material_type",
                "ext_matl_group",
                "product_group",
                "productline",
                "customer_group",
                "HMG_PN",
            ],
            dropna=False,  # To avoid deleting rows with missing values
        )
        .agg({"quantity": "sum", "totsaleslc": "sum"})
        .reset_index()
    )
    return df


def filter_high_runner(df):
    # Filter data for Representative PN
    df = df[df["recordtype"] == "F"]
    df = df[df["material_type"].isin(["FERT", "HALB"])]
    df = df[(df["totsaleslc"] > 5e7) | (df["totsaleslc"] < -5e7)]  # over +/- 50M KRW
    return df


def sort_data(df):
    df = df.sort_values(
        by=["fy", "product_hierarchy", "totsaleslc"], ascending=[True, True, False]
    )
    return df


def filter_first_occurrence(df):
    # Filter the first occurrence of each category
    df = df.groupby(["fy", "product_hierarchy"]).head(1)
    return df


def select_rename_cols(df):
    # Select columns
    df = df[
        [
            # Key
            "fy",
            "recordtype",
            "product_hierarchy",
            # Representative PN and HMG_PN
            "product",
            "HMG_PN",
        ]
    ]
    # Rename columns
    df = df.rename(columns={"product": "representative_pn"})
    return df


def drop_HMG_PN(df):  # df: full sales data
    # to be replaced by HMG_PN of representative PN
    df = df.drop(columns="HMG_PN")
    return df


def add_representative_pn(df, df_reprsentative_pn):
    df = df.merge(
        df_reprsentative_pn, how="left", on=["fy", "recordtype", "product_hierarchy"]
    )
    return df


def filter_material_type(df):
    # Remove representative_pn if material type is HAWA or ROH
    df["representative_pn"] = df["representative_pn"].where(
        df["material_type"].isin(["FERT", "HALB"])
    )
    return df


def main():

    # Filenames
    input_file = path / "output" / "Sales BU CT_with meta.csv"

    output_1 = path / "output" / "Sales high runner per PH.csv"
    output_2 = path / "output" / "Sales high runner PN.csv"
    output_3 = path / "output" / "Sales with representative PN.csv"

    # Read data
    df = pd.read_csv(input_file)

    # Process data
    high_runner_sales = df.pipe(aggregate_data).pipe(filter_high_runner).pipe(sort_data)

    high_runner_pn = high_runner_sales.pipe(filter_first_occurrence)

    representative_pn = select_rename_cols(high_runner_pn)

    sales_with_representative_pn = (
        df.pipe(drop_HMG_PN)  # to be replaced by HMG_PN of representative PN
        .pipe(add_representative_pn, representative_pn)
        .pipe(
            # Remove representative_pn if material type is HAWA or ROH
            filter_material_type
        )
    )

    # Write data
    high_runner_sales.to_csv(output_1, index=False)
    high_runner_pn.to_csv(output_2, index=False)
    sales_with_representative_pn.to_csv(output_3, index=False)
    print("files are created.")


if __name__ == "__main__":
    main()
