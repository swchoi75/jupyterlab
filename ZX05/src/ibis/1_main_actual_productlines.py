import pandas as pd
import ibis
from ibis import selectors as s
from ibis import _
from pathlib import Path
from janitor import clean_names
from common_functions import (
    add_col_coom,
    add_col_function_2,
    filter_pl_fix,
    filter_pl_var,
    add_race_item,
    add_pl_acc_info,
    remove_unnecessary_cols,
)


# Path
path = Path(__file__).parent.parent.parent


# Functions
def main():
    # Filenames
    db_file = path / "db" / "PL_2024.csv"

    acc_master_file = path / "meta" / "0000_TABLE_MASTER_Acc level.csv"
    cc_general_file = path / "meta" / "0000_TABLE_MASTER_Cost center_general.csv"
    cc_hierarchy_file = path / "meta" / "0000_TABLE_MASTER_Cost center_hierarchy.csv"
    coom_master_file = path / "meta" / "0004_TABLE_MASTER_COOM_2024.csv"
    poc_master_file = path / "meta" / "POC.csv"

    output_1 = path / "output" / "PL fix costs.csv"
    output_2 = path / "output" / "PL var costs.csv"

    # Read DB file
    df = pd.read_csv(db_file, dtype={"costctr": str})
    pl = ibis.memtable(df)

    # Process numeric columns
    pl = pl.mutate(s.across(["actual", "plan", "target"], _ / -1000)).mutate(
        delta_to_plan=(_.actual - _.plan).round(3)
    )

    # Add volume difference
    pl = pl.mutate(volume_difference=(_.plan - _.target).round(3))

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

    # ### Data wrangling

    # Join tables to get master data
    pl = (
        pl.join(acc, pl.gl_accounts == acc.account_no_, how="left")
        .join(cc, pl.costctr == cc.cctr, how="left")
        .join(poc, cc.pctr == poc.profit_center, how="left")
        .drop("cctr", "account_no_", "cctr_right", "pctr")
    )
    pl = pl.join(
        coom,
        (pl.costctr == coom.cctr) & (pl.gl_accounts == coom.account_no_),
        how="left",
    ).drop("cctr", "account_no_")

    # Process COOM data for fix and variable costs
    # Extract function from cost center hierarchy level 3
    pl = pl.pipe(add_col_coom).pipe(add_col_function_2)

    # PL fix costs
    pl_fix = (
        pl.pipe(filter_pl_fix)
        .pipe(add_pl_acc_info)
        .pipe(add_race_item)
        .pipe(remove_unnecessary_cols)
    )

    # PL var costs
    pl_var = pl.pipe(filter_pl_var)

    # Add LDC / MDC
    pl_var = pl_var.mutate(
        ldc_mdc=ibis.case()
        .when(pl_var.costctr[:1] == "8", "Start up costs")  # CC that starts with "8"
        .when(
            (pl_var.function_2 == "FGK") & (pl_var.acc_lv2 == "299 Total Labor Costs"),
            "LDC",
        )
        .when(
            (pl_var.function_2 == "FGK") & (pl_var.acc_lv2 == "465 Cost of materials"),
            "MDC",
        )
        .when(True, "NA")
        .end()
    )

    # Add account information
    pl_var = pl_var.mutate(
        ce_text=ibis.case()
        # LDC
        .when(
            (pl_var.gl_accounts == "K250") | (pl_var.gl_accounts == "K256"),
            "120 Premium wages",
        )
        .when(
            pl_var.acc_lv1 == "158 Social benefit rate wages variable", "158 SLB wages"
        )
        .when(pl_var.acc_lv2 == "299 Total Labor Costs", "115 Direct labor")
        # MDC
        .when(True, pl_var.acc_lv1)
        .end()
    )

    # Remove unnecessary columns
    pl_var = pl_var.pipe(remove_unnecessary_cols)

    # Write data
    # pl_fix.to_pandas().to_csv(output_1, index=False)
    # pl_var.to_pandas().to_csv(output_2, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
