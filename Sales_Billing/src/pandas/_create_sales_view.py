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
    type_mapping = {
        "FERT": "01",
        "HAWA": "02",
        "HALB": "03",
        "ROH": "04",
    }
    return type_mapping.get(row["Type"], None)


def add_material_group(row):
    customer_mapping = {
        "HMC": "183",
        "KMC": "191",
        "KIA": "191",
        "GM": "105",
        "RSM": "193",
        "Ssangyong": "180",
        "SYM": "180",
        "Others": "199",
        "MOBIS": "317",
        "Mobis": "317",
        "MANDO": "384",
        "Mando": "384",
        "Webasto": "376",
        "VT": "360",
        "Conti": "394",
    }

    return customer_mapping.get(row["Customer"], "Unknown")


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
