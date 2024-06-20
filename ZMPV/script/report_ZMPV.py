import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def zmpv_ico(df):
    # Filter Intercompany
    return df[df["outs_ic"] == "IC"]


def zmpv_ppv(df):
    # Filter Net PPV
    df = df[(df["net_pm_ppv"] < -10000) | (df["net_pm_ppv"] > 10000)]
    return df


def zmpv_fx(df):
    # Filter FX Material (= former PPV FX)
    df = df[(df["fx_effect"] < -10000) | (df["fx_effect"] > 10000)]
    return df


def zmpv_loco(df):
    # Filter LOCO
    df = df[
        df["std_tool_c"].abs() + df["std_freigh"].abs() + df["std_customs"].abs()
        > 10000
    ]
    return df


def zmpv_smd(df):
    # Filter SMD Outsourcing
    df = df[(df["vendor"] == "0009085884") | (df["vendor"] == "0009072686")]

    # filter out zero quantity
    df = df.fillna(0).infer_objects(copy=False)
    df = df.astype({"gr_quantity": "int"})
    df = df[df["gr_quantity"] != 0]

    return df


def main():

    # Variable
    from variable_year import year

    # Filenames
    db_file = path / "db" / f"ZMPV_{year}.parquet"

    output_1 = path / "output" / "ZMPV ICO purchase.csv"
    output_2 = path / "output" / "ZMPV Net PPV.csv"
    output_3 = path / "output" / "ZMPV FX Material.csv"
    output_4 = path / "output" / "ZMPV STD LOCO.csv"
    output_5 = path / "output" / "ZMPV SMD outsourcing.csv"

    # Read data
    df = pd.read_parquet(db_file)

    # Process data
    df_1 = zmpv_ico(df)
    df_2 = zmpv_ppv(df)
    df_3 = zmpv_fx(df)
    df_4 = zmpv_loco(df)
    df_5 = zmpv_smd(df)

    # Write data
    df_1.to_csv(output_1, index=False, na_rep="0")
    df_2.to_csv(output_2, index=False, na_rep="0")
    df_3.to_csv(output_3, index=False, na_rep="0")
    df_4.to_csv(output_4, index=False, na_rep="0")
    df_5.to_csv(output_5, index=False, na_rep="0")
    print("Files are updated")


if __name__ == "__main__":
    main()
