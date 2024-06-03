import pandas as pd
from pathlib import Path
from janitor import clean_names
from commonfunctions import (
    read_db,
    process_numeric_columns,
    remove_unnecessary_columns,
    read_master_data,
    master_cc,
    process_master_data_1,
)
from cfcosts import add_ce_text


# Path
path = Path(__file__).parent.parent


# Functions
def main():
    # Filenames
    db_file = path / "db" / "CF_2024.csv"
    meta_acc = path / "meta" / "0000_TABLE_MASTER_Acc level.csv"
    meta_cc_general = path / "meta" / "0000_TABLE_MASTER_Cost center_general.csv"
    meta_cc_hierarchy = path / "meta" / "0000_TABLE_MASTER_Cost center_hierarchy.csv"
    output_file = path / "output" / "CF costs.csv"

    # Read data
    df = read_db(db_file)
    df_acc = read_master_data(meta_acc)
    df_cc_general = read_master_data(meta_cc_general)
    df_cc_hierarchy = read_master_data(meta_cc_hierarchy)
    df_cc = master_cc(df_cc_general, df_cc_hierarchy)

    # Process data
    df = (
        df.pipe(process_numeric_columns)
        .pipe(process_master_data_1, df_cc, df_acc)
        .pipe(add_ce_text)
        .pipe(remove_unnecessary_columns)
    )

    # Write data
    # df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
