import pandas as pd
import ibis
from ibis import selectors as s
from ibis import _
from pathlib import Path

# ibis.options.interactive = True


# Path
path = Path(__file__).parent.parent.parent


# Functions


def main():

    # Filenames
    input_file = path / "db" / "ZVAR_2024.csv"
    output_1 = path / "output" / "ZVAR Activity variance.csv"
    output_2 = path / "output" / "ZVAR Material usage variance_full.csv"
    output_3 = path / "output" / "ZVAR Material usage variance_major.csv"
    output_4 = path / "output" / "ZVAR no post others.csv"

    # Read DB file
    df = pd.read_csv(input_file, dtype={"order": str, "cost_ctr": str})
    zvar = ibis.memtable(df)

    # Process data
    # Assign column names into variable
    cols_common = [
        "order",
        "material",
        "cost_elem_",
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
        "per",  # "per" is period
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

    # Predicates to filter data
    predicate_activity = (_.acttyp != ibis.NA) & (_.no_post == ibis.NA)
    predicate_material = (_.acttyp == ibis.NA) & (_.no_post == ibis.NA)
    predicate_others = (_.acttyp == ibis.NA) & (_.no_post != ibis.NA)

    # Activity variance
    zvar_activity = zvar[cols_common + cols_activity].filter(predicate_activity)

    # Material variance
    zvar_mat_full = (
        zvar[cols_common + cols_material].filter(predicate_material)
        # Filter out zero values
        .filter(~((_.mvsu == 0) & (_.mvqn == 0) & (_.mvpr == 0) & (_.bset == 0)))
    )

    # Material variance
    zvar_mat_major = zvar_mat_full.filter(
        (_.mvsu + _.mvqn + _.mvpr + _.bset).abs() > 10**5
    )

    # Other variance
    zvar_oth_var = zvar[cols_common + cols_others].filter(predicate_others)

    # Write data
    zvar_activity.to_pandas().to_csv(output_1, index=False, na_rep="0")
    zvar_mat_full.to_pandas().to_csv(output_2, index=False, na_rep="0")
    zvar_mat_major.to_pandas().to_csv(output_3, index=False, na_rep="0")
    zvar_oth_var.to_pandas().to_csv(output_4, index=False, na_rep="0")


print("Files are created")


if __name__ == "__main__":
    main()
