import pandas as pd
from pathlib import Path
from commonfunctions import (
    read_db,
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
from plvarcosts import filter_var_cost, filter_scm, add_ldc_mdc, add_ce_text


# Path
path = Path(__file__).parent.parent


# Functions
def main():
    # Filenames
    db_file = path / "db" / "PL_2024.csv"
    meta_acc = path / "meta" / "0000_TABLE_MASTER_Acc level.csv"
    meta_cc_general = path / "meta" / "0000_TABLE_MASTER_Cost center_general.csv"
    meta_cc_hierarchy = path / "meta" / "0000_TABLE_MASTER_Cost center_hierarchy.csv"
    meta_coom = path / "meta" / "0004_TABLE_MASTER_COOM_2024.csv"
    meta_poc = path / "meta" / "POC.csv"
    output_file = path / "output" / "PL var SCM costs.csv"

    # Read data
    df = read_db(db_file)
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
        .pipe(filter_scm)
        .pipe(remove_s90xxx_accounts)
        .pipe(add_ldc_mdc)
        .pipe(add_ce_text)
        .pipe(remove_unnecessary_columns)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
