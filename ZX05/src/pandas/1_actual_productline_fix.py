from pathlib import Path
from common_function import (
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
from plfixcosts import (
    filter_fix_costs,
    add_ce_text,
    add_race_item,
)


# Path
path = Path(__file__).parent.parent.parent


def main():

    # Variables
    from common_variable import year

    # Filenames

    db_file = path / "db" / f"PL_{year}.csv"
    meta_acc = path / "meta" / "0000_TABLE_MASTER_Acc level.csv"
    meta_cc_general = path / "meta" / "0000_TABLE_MASTER_Cost center_general.csv"
    meta_cc_hierarchy = path / "meta" / "0000_TABLE_MASTER_Cost center_hierarchy.csv"
    meta_coom = path / "meta" / f"0004_TABLE_MASTER_COOM_{year}.csv"
    meta_poc = path / "meta" / "POC.csv"
    output_file = path / "output" / "PL fix costs.csv"

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
        .pipe(filter_fix_costs)
        .pipe(remove_s90xxx_accounts)
        .pipe(add_ce_text)
        .pipe(add_race_item)
        .pipe(remove_unnecessary_columns)
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
