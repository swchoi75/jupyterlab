import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


# Functions


def main():

    # Filenames
    input_1 = path / "output" / "1_actual_sales.csv"
    input_2 = path / "output" / "3_budget_std_costs.csv"
    output_file = path / "output" / "4_actual_and_budget_sales.csv"

    # Read data
    df_act = pd.read_csv(input_1)
    df_bud = pd.read_csv(input_2)

    # Process data
    df = pd.concat([df_act.assign(version="Actual"), df_bud.assign(version="Budget")])

    list_of_columns = [
        "version",
        "year",
        "month",
        # "period",
        "profit_ctr",
        "recordtype",
        "cost_elem",
        "account_class",
        "plnt",
        "product",
        "material_description",
        "sold_to_party",
        "sold_to_name_1",
        "qty",
        "sales_lc",
        "std_costs",
    ]
    df = df[list_of_columns]  # reorder columns

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
