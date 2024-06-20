import pandas as pd
import ibis
from ibis import selectors as s
from ibis import _
from pathlib import Path
from janitor import clean_names
from common_functions import (
    add_cf_acc_info,
    remove_unnecessary_cols,
)


# Path
path = Path(__file__).parent.parent.parent


# Functions
def main():

    # Filenames
    input_file = path / "db" / "CF_2024.csv"

    acc_master_file = path / "meta" / "0000_TABLE_MASTER_Acc level.csv"
    cc_general_file = path / "meta" / "0000_TABLE_MASTER_Cost center_general.csv"
    cc_hierarchy_file = path / "meta" / "0000_TABLE_MASTER_Cost center_hierarchy.csv"

    output_file = path / "output" / "CF costs.csv"

    # Read DB file
    df = pd.read_csv(input_file, dtype={"costctr": str})
    cf = ibis.memtable(df)

    # Process numeric columns
    cf = cf.mutate(s.across(["actual", "plan", "target"], _ / -1000)).mutate(
        delta_to_plan=(_.actual - _.plan).round(3)
    )

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
    cf = (
        cf.join(acc, cf.gl_accounts == acc.account_no_, how="left")
        .join(cc, cf.costctr == cc.cctr, how="left")
        .drop("cctr", "account_no_", "cctr_right")
    )

    # Add account information
    cf = cf.pipe(add_cf_acc_info).pipe(remove_unnecessary_cols)

    # Write data
    # cf.to_pandas().to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
