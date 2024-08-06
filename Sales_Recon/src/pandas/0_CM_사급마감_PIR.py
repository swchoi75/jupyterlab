import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def rename_columns(df):
    df = df.rename(
        columns={
            "unnamed_14": "pir_net_price",
            "unnamed_22": "sa_net_price",
            "unnamed_25": "unit",
            "unnamed_35": "sa_valid_from",
            "info_record_1": "pir_valid_from",
            "sa_#": "sa_number",
        }
    )
    return df


def change_data_type(df):
    # remove NA values
    df["sa_net_price"] = df["sa_net_price"].fillna(0)
    # change from float to integer
    df["sa_net_price"] = df["sa_net_price"].astype(int)
    return df


def select_columns(df, selected_columns):
    df = df[selected_columns]
    return df


def main():

    # Variables
    from common_variable import year, month

    # Filenames
    input_file = path / "data" / "VAN CM" / f"SA_{year}{month}.xls"
    output_1 = path / "data" / "VAN CM" / "SA_LS Automotive.csv"
    output_2 = path / "data" / "VAN CM" / "SA_MOBASE.csv"

    # Read data
    df = pd.read_csv(
        input_file,
        skiprows=9,
        sep="\t",
        encoding="UTF-16LE",
        skipinitialspace=True,
        thousands=",",
        engine="python",
    ).clean_names(strip_accents=False)

    # Process data
    df = (
        df.pipe(rename_columns)
        .pipe(change_data_type)
        .pipe(
            select_columns,
            [
                "vendor",
                "vendor_name",
                "material_no",
                "mtye",
                "sa_net_price",
                "unit",
                "sa_valid_from",
                "sa_number",
            ],
        )
    )

    ## Filter dataframe
    df = df[df["sa_net_price"] != 0]
    df_ls = df[df["vendor"] == "9139976"]
    df_mo = df[df["vendor"] == "9082855"]

    # Write data
    df_ls.to_csv(output_1, index=False)
    df_mo.to_csv(output_2, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
