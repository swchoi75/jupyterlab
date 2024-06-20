import pandas as pd
import ibis
from ibis import selectors as s
from ibis import _
from pathlib import Path
from janitor import clean_names
from common_functions import (
    read_data,
    extract_text,
    process_num_cols,
    add_cf_acc_info,
    remove_unnecessary_cols,
)

# ibis.options.interactive = True


# Path
path = Path(__file__).parent.parent.parent


# Functions
def main():

    # Input data
    input_file = path / "data" / "FC_2024/CF_FC2_test.dat"

    acc_master_file = path / "meta" / "0000_TABLE_MASTER_Acc level.csv"
    cc_general_file = path / "meta" / "0000_TABLE_MASTER_Cost center_general.csv"
    cc_hierarchy_file = path / "meta" / "0000_TABLE_MASTER_Cost center_hierarchy.csv"

    output_file = path / "output" / "CF costs FC_test.csv"

    # Read data
    df = read_data(input_file).pipe(extract_text)
    tbl = ibis.memtable(df)
    tbl = tbl.filter(_.gl_accounts != ibis.NA).select(
        "costctr", "gl_accounts", s.numeric()
    )
    tbl = tbl.pipe(process_num_cols)

    # Read master data
    df_acc = pd.read_csv(acc_master_file, dtype=str).clean_names()
    df_cc_general = pd.read_csv(cc_general_file, dtype=str).clean_names()
    df_cc_hierarchy = pd.read_csv(cc_hierarchy_file, dtype=str).clean_names()

    acc = ibis.memtable(df_acc)
    cc_general = ibis.memtable(df_cc_general)
    cc_hierarchy = ibis.memtable(df_cc_hierarchy)
    cc = cc_general.join(cc_hierarchy, "cctr", how="left")

    # Process data

    # Join tables to get master data
    tbl = (
        tbl.join(acc, tbl.gl_accounts == acc.account_no_, how="left")
        .join(cc, tbl.costctr == cc.cctr, how="left")
        .drop("cctr", "account_no_", "cctr_right")
    )

    tbl = tbl.pipe(add_cf_acc_info).pipe(remove_unnecessary_cols)

    # Write data
    # tbl.to_pandas().to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
