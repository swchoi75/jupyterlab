import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Function
def read_csv_file(path, fc_version):
    df = pd.read_csv(path)
    df["version"] = df["version"].str.replace("FC", fc_version)
    df = df[["version"] + [col for col in df.columns if col != "version"]]
    return df


def main():

    # Filenames
    input_fc10 = path / "output" / "Monthly Spending FC10+2.csv"
    input_fc6 = path / "output" / "Monthly Spending FC6+6.csv"
    input_fc4 = path / "output" / "Monthly Spending FC4+8.csv"
    input_fc3 = path / "output" / "Monthly Spending FC3+9.csv"
    input_fc2 = path / "output" / "Monthly Spending FC2+10.csv"
    output_file = path / "output" / "Monthly Spending merged.csv"

    # Process data
    fc10 = read_csv_file(input_fc10, "FC10+2")
    fc6 = read_csv_file(input_fc6, "FC6+6").loc[
        lambda df: df["version"].str.contains("FC")
    ]
    fc4 = read_csv_file(input_fc4, "FC4+8").loc[
        lambda df: df["version"].str.contains("FC")
    ]
    fc3 = read_csv_file(input_fc3, "FC3+9").loc[
        lambda df: df["version"].str.contains("FC")
    ]
    fc2 = read_csv_file(input_fc2, "FC2+10").loc[
        lambda df: df["version"].str.contains("FC")
    ]

    merged = pd.concat([fc10, fc6, fc4, fc3, fc2], axis="rows")

    # Write data
    merged.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
