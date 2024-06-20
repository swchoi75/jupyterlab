# In Terminal, "pip install ibis-framework[duckdb] pyjanitor"
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
    add_col_coom,
    add_col_function_2,
    filter_pl_fix,
    add_race_item,
    add_pl_acc_info,
    remove_unnecessary_cols,
)

# ibis.options.interactive = True


# Path
path = Path(__file__).parent.parent.parent


# Functions
def main():

    # Filename
    input_file = path / "data" / "FC_2024/PL_FC2_test.dat"

    acc_master_file = path / "meta" / "0000_TABLE_MASTER_Acc level.csv"
    cc_general_file = path / "meta" / "0000_TABLE_MASTER_Cost center_general.csv"
    cc_hierarchy_file = path / "meta" / "0000_TABLE_MASTER_Cost center_hierarchy.csv"
    coom_master_file = path / "meta" / "0004_TABLE_MASTER_COOM_2024.csv"
    poc_master_file = path / "meta" / "POC.csv"

    output_file = path / "output" / "PL fix costs FC.csv"

    # Read data
    df = read_data(input_file).pipe(extract_text)
    tbl = ibis.memtable(df)
    tbl = tbl.select("costctr", "gl_accounts", s.numeric()).pipe(process_num_cols)

    # Read master data
    df_acc = pd.read_csv(acc_master_file, dtype=str).clean_names()
    df_cc_general = pd.read_csv(cc_general_file, dtype=str).clean_names()
    df_cc_hierarchy = pd.read_csv(cc_hierarchy_file, dtype=str).clean_names()
    df_coom = pd.read_csv(coom_master_file, dtype=str, usecols=[0, 1, 2]).clean_names()
    df_poc = pd.read_csv(poc_master_file, dtype=str).clean_names()

    # Convert dataframe to ibis table
    acc = ibis.memtable(df_acc)
    cc_general = ibis.memtable(df_cc_general)
    cc_hierarchy = ibis.memtable(df_cc_hierarchy)
    cc = cc_general.join(cc_hierarchy, "cctr", how="left")
    coom = ibis.memtable(df_coom)
    poc = ibis.memtable(df_poc)

    # Process data

    # Join tables to get master data
    tbl = (
        tbl.join(acc, tbl.gl_accounts == acc.account_no_, how="left")
        .join(cc, tbl.costctr == cc.cctr, how="left")
        .join(poc, cc.pctr == poc.profit_center, how="left")
        .drop("cctr", "account_no_", "cctr_right", "pctr")
    )
    tbl = tbl.join(
        coom,
        (tbl.costctr == coom.cctr) & (tbl.gl_accounts == coom.account_no_),
        how="left",
    ).drop("cctr", "account_no_")

    # PL fix costs
    tbl = (
        tbl.pipe(add_col_coom)
        .pipe(add_col_function_2)
        .pipe(filter_pl_fix)
        .pipe(add_pl_acc_info)
        .pipe(add_race_item)
        .pipe(remove_unnecessary_cols)
    )

    # Write data
    # tbl.to_pandas().to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
