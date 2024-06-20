import pandas as pd
from pathlib import Path
from common_function import (
    process_numeric_columns,
    remove_unnecessary_columns,
    read_master_data,
    master_cc,
    master_coom,
    process_master_data_2,
    add_vol_diff,
    split_fix_var,
    get_cc_function,
    remove_s90xxx_accounts,
)
from plvarcosts import filter_var_cost, add_ldc_mdc, add_ce_text


# Path
path = Path(__file__).parent.parent


# Functions
def filter_overtime_costs(df):
    # Define overtime accounts
    overtime_accounts = [
        # Variable CC
        "K250",
        "K256",
        # Fix CC
        "K2501",
        "K2561",
        # Central function
        "K301",
        "K302",
    ]

    df = df.loc[df["gl_accounts"].isin(overtime_accounts)]
    return df


def main():

    # Variables
    from common_variable import year

    # Filenames
    db_file_2020 = path / "db" / "PL_2020.csv"
    db_file_2021 = path / "db" / "PL_2021.csv"
    db_file_2022 = path / "db" / "PL_2022.csv"
    db_file_2023 = path / "db" / "PL_2023.csv"
    db_file_2024 = path / "db" / "PL_2024.csv"

    meta_acc = path / "meta" / "0000_TABLE_MASTER_Acc level.csv"
    meta_cc_general = path / "meta" / "0000_TABLE_MASTER_Cost center_general.csv"
    meta_cc_hierarchy = path / "meta" / "0000_TABLE_MASTER_Cost center_hierarchy.csv"
    meta_coom = path / "meta" / f"0004_TABLE_MASTER_COOM_{year}.csv"
    meta_poc = path / "meta" / "POC.csv"

    output_file = path / "output" / "PL var OT costs 2020-2024 YTD.csv"

    # Read data
    pl_2020 = pd.read_csv(db_file_2020, dtype="str").clean_names()
    pl_2021 = pd.read_csv(db_file_2021, dtype="str").clean_names()
    pl_2022 = pd.read_csv(db_file_2022, dtype="str").clean_names()
    pl_2023 = pd.read_csv(db_file_2023, dtype="str").clean_names()
    pl_2024 = pd.read_csv(db_file_2024, dtype="str").clean_names()
    df = pd.concat([pl_2020, pl_2021, pl_2022, pl_2023, pl_2024])

    df_acc = read_master_data(meta_acc)
    df_cc_general = read_master_data(meta_cc_general)
    df_cc_hierarchy = read_master_data(meta_cc_hierarchy)
    df_cc = master_cc(df_cc_general, df_cc_hierarchy)
    df_coom = master_coom(meta_coom)
    df_poc = read_master_data(meta_poc)

    # Process data
    df = (
        df.pipe(process_numeric_columns)
        .pipe(add_vol_diff)
        .pipe(process_master_data_2, df_cc, df_acc, df_coom, df_poc)
        .pipe(split_fix_var)
        .pipe(get_cc_function)
        .pipe(filter_var_cost)
        .pipe(remove_s90xxx_accounts)
        .pipe(add_ldc_mdc)
        .pipe(add_ce_text)
        .pipe(remove_unnecessary_columns)
        .pipe(filter_overtime_costs)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
