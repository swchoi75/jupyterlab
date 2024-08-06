import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def filter_last_date(df):
    last_date = df["date"].max()  # find the last date
    df = df[df["date"] == last_date]  # filter the DataFrame
    return df


def add_acc_assig_grp(row):
    if row["Type"] == "FERT":
        return "01"
    elif row["Type"] == "HAWA":
        return "02"
    elif row["Type"] == "HALB":
        return "03"
    elif row["Type"] == "ROH":
        return "04"


def add_material_group(row):
    if row["Customer"] == "HMC":
        return "183"
    elif row["Customer"] == "KMC":
        return "191"
    elif row["Customer"] == "KIA":
        return "191"
    elif row["Customer"] == "GM":
        return "105"
    elif row["Customer"] == "RSM":
        return "193"
    elif row["Customer"] == "Ssangyong":
        return "180"
    elif row["Customer"] == "SYM":
        return "180"
    elif row["Customer"] == "Others":
        return "199"
    elif row["Customer"] == "MOBIS":
        return "317"
    elif row["Customer"] == "Mobis":
        return "317"
    elif row["Customer"] == "MANDO":
        return "384"
    elif row["Customer"] == "Mando":
        return "384"
    elif row["Customer"] == "Webasto":
        return "376"
    elif row["Customer"] == "VT":
        return "360"
    elif row["Customer"] == "Conti":
        return "394"


def change_format(df):
    df["plant"] = "0180"
    df["sales_org"] = "5668"
    df["acc_assig_grp"] = df.apply(add_acc_assig_grp, axis="columns")
    df["material_group1"] = df.apply(add_material_group, axis="columns")
    return df


def select_columns(df, selected_columns):
    df = df[selected_columns]
    return df


def main():

    # Filenames
    input_file = path / "data" / "Sales View Creation Request.csv"
    output_1 = "C:" / "LSMW" / "Sales_view.txt"
    output_2 = path / "output" / "Sales_view.txt"

    # Read data
    df = pd.read_csv(input_file).clean_names()

    # Process data
    df = (
        df.pipe(filter_last_date)
        .pipe(change_format)
        .pipe(
            select_columns,
            [
                "plant",
                "sales_org",
                "material_number",
                "acc_assig_grp",
                "material_group1",
                "material_group2",
                "material_group3",
            ],
        )
    )

    # Write data
    df.to_csv(output_1, index=False)
    df.to_csv(output_2, index=False)

    print("Files are created")


if __name__ == "__main__":
    main()
