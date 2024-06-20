import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Functions
def zvar_act(df, cols_common, cols_activity):
    # Select activity variance
    df = df.loc[
        ~df["acttyp"].isna() & df["no_post"].isna(), cols_common + cols_activity
    ]
    return df


def zvar_mat_full(df, cols_common, cols_material):
    # Select material usage variance
    df = df.loc[df["acttyp"].isna() & df["no_post"].isna(), cols_common + cols_material]
    # Filter out zero values
    df = df.loc[
        ~((df["mvqn"] == 0) & (df["mvsu"] == 0) & (df["mvpr"] == 0) & (df["bset"] == 0))
    ]
    return df


def zvar_mat_major(df, low_limit):
    # Select material usage variance
    df = df.loc[(df["mvqn"] + df["mvsu"] + df["mvpr"] + df["bset"]).abs() > low_limit]
    return df


def zvar_oth(df, cols_common, cols_others):
    # Select other variance
    df = df.loc[df["acttyp"].isna() & ~df["no_post"].isna(), cols_common + cols_others]
    return df


def main():

    # Variable
    from variable_year import year

    low_limit = 10**5  # 100,000
    cols_common = [
        "order",
        "material",
        "cost_elem",
        "comaterial",
        "cost_ctr",
        "acttyp",
        "d_c",
        "um",
        "act_costs",
        "targ_costs",
        "var_amount",
        "actual_qty",
        "target_qty",
        "qty_varian",
        "product_hierarchy",
        "blk_ind",
        "no_post",
        "period",
        "year",
        "itm",
        "type",
        "cat",
        "prctr_cc",
        "description",
        "profit_ctr",
    ]

    cols_activity = ["avsu", "avqn", "avpr"]
    cols_material = ["mvsu", "mvqn", "mvpr", "bset"]  # "bset" is bulk variance
    cols_others = ["pvar"]  # "pvar" is total variance

    # Filenames
    db_file = path / "db" / f"ZVAR_{year}.parquet"
    output_1 = path / "output" / "ZVAR Activity variance.csv"
    output_2 = path / "output" / "ZVAR Material usage variance_full.csv"
    output_3 = path / "output" / "ZVAR Material usage variance_major.csv"
    output_4 = path / "output" / "ZVAR no post others.csv"

    # Read data
    df = pd.read_parquet(db_file)

    # Process data
    df_activity = zvar_act(df, cols_common, cols_activity)
    df_full = zvar_mat_full(df, cols_common, cols_material)
    df_major = zvar_mat_major(df_full, low_limit)
    df_others = zvar_oth(df, cols_common, cols_others)

    # Write data
    df_activity.to_csv(output_1, index=False, na_rep="0")
    df_full.to_csv(output_2, index=False, na_rep="0")
    df_major.to_csv(output_3, index=False, na_rep="0")
    df_others.to_csv(output_4, index=False, na_rep="0")
    print("Files are created")


if __name__ == "__main__":
    main()
