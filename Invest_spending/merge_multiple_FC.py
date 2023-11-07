import pandas as pd
from pathlib import Path


# Path
path = Path.cwd()


# Function
def read_csv_file(path, fc_version):
    df = pd.read_csv(path)
    df["version"] = df["version"].str.replace("FC", fc_version)
    df = df[["version"] + [col for col in df.columns if col != "version"]]
    return df


# Input data
fc10_path = path / "output" / "Monthly Spending FC10+2.csv"
fc6_path = path / "output" / "Monthly Spending FC6+6.csv"
fc4_path = path / "output" / "Monthly Spending FC4+8.csv"
fc3_path = path / "output" / "Monthly Spending FC3+9.csv"
fc2_path = path / "output" / "Monthly Spending FC2+10.csv"


# Process data
fc10 = read_csv_file(fc10_path, "FC10+2")
fc6 = read_csv_file(
    fc6_path, "FC6+6").loc[lambda df: df["version"].str.contains("FC")]
fc4 = read_csv_file(
    fc4_path, "FC4+8").loc[lambda df: df["version"].str.contains("FC")]
fc3 = read_csv_file(
    fc3_path, "FC3+9").loc[lambda df: df["version"].str.contains("FC")]
fc2 = read_csv_file(
    fc2_path, "FC2+10").loc[lambda df: df["version"].str.contains("FC")]

merged = pd.concat([fc10, fc6, fc4, fc3, fc2], axis="rows")


# Output data
merged.to_csv(path / "output" / "Monthly Spending merged.csv", index=False)
