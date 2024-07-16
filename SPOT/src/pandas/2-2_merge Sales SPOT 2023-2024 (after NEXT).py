import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Functions
def pivot_data_longer(df, list_of_columns):
    return (
        df.melt(
            id_vars=list_of_columns,
            # value_vars=
            var_name="key",
            value_name="values",
        ).assign(
            year=lambda row: row["key"].str.extract(r"(\d{4})"),
            measure=lambda row: row["key"]
            .str.extract(r"(sales|volume|price)")
            .astype(str),
        )
        # .drop(columns=["key"])
    )


def pivot_data_wider(df, list_of_columns):
    df = df.pivot_table(
        index=list_of_columns,
        values="values",
        columns="measure",
        aggfunc="sum",
    ).reset_index()
    return df


def remove_zero_na(df):
    # Remove zero values
    df = df.drop(df[(df["sales"] == 0.0) & (df["volume"] == 0.0)].index)
    # Remove na values
    df = df[df["year"].notna()]
    return df


def main():
    # Variable
    year = 2024

    list_of_cols = [
        "source",
        "division",  # after project NEXT
        "business_unit",
        "product_line",  # after project NEXT
        "sourcing_cust_group",  # after project NEXT
        "sourcing_cust_cat_name",  # after project NEXT
        "business_type",
        "om_status",
        "sales_plant",  # after project NEXT
        "sales_plant_pivot",  # after project NEXT
        "kam_office_sour_cus",  # after project NEXT
        "final_customer_group",  # after project NEXT
        "country_final_cust",  # after project NEXT
        "country_prod_plant",  # after project NEXT
        "sourcing_customer",
        "sourcing_decis_date",
        "project_title",
        "product_group",  # after project NEXT
        "product_group_pivot",  # after project NEXT
        "product_hierarchy",  # after project NEXT
        "product_hierarchy_pivot",  # after project NEXT
        "project_id",
        "line_item_description",  # after project NEXT
        "sop_line_item",
        "won_lost_exit_date",
    ]

    # Path
    input_file = path / "output" / f"SPOT_combined_{year}.csv"
    output_file = path / "output" / f"SPOT_merged_{year}.csv"

    # Read data
    df = pd.read_csv(input_file)

    # Process data
    df = (
        df.pipe(pivot_data_longer, list_of_cols)
        .pipe(pivot_data_wider, list_of_cols + ["year"])
        .pipe(remove_zero_na)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
