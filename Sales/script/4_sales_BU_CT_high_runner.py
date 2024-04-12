import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


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


def filter_data(df):
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


def full_data(df, filename):
    # Full sales data
    df = pd.read_csv(filename).drop(
        columns="HMG_PN"  # to be replaced by HMG_PN of representative PN
    )
    return df


def representative_df(df):
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
    ].rename(columns={"product": "representative_pn"})
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
    output_file = path / "output" / "Sales high runner per PH.csv"
    result_file = path / "output" / "Sales high runner PN.csv"
    full_sales = path / "output" / "Sales with representative PN.csv"

    # Read data
    df = pd.read_csv(input_file)
    df = df.pipe(aggregate_data).pipe(filter_data).pipe(sort_data)
    result = df.pipe(filter_first_occurrence)
    df_representative = representative_df(result)
    df_full = df.pipe(full_data, input_file)
    df_full = df_full.pipe(add_representative_pn, df_representative).pipe(
        filter_material_type
    )

    # Write data
    df.to_csv(output_file, index=False)
    result.to_csv(result_file, index=False)
    df_full.to_csv(full_sales, index=False)
    print("files are created.")


if __name__ == "__main__":
    main()
