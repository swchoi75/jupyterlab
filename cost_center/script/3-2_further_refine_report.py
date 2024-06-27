import pandas as pd
import numpy as np
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def main():

    # Filenames
    input_file = path / "output" / "3-1_fix_act_to_plan_subtotal.csv"
    output_file = path / "output" / "3-2_further_refine_report.csv"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data
    ## fill blank

    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    df["acc_lv1"] = np.where(df["acc_lv1"] == "", df["acc_lv2"], df["acc_lv1"])

    df["account_description"] = np.where(
        df["account_description"] == "", df["acc_lv1"], df["account_description"]
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
