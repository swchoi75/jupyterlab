import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
# path = Path('datasets/home-dataset/')
path = Path(__file__).parent
data_path = path / "data/"


# List of multiple excel files
xls_files = [
    file for file in data_path.iterdir() if file.is_file() and file.suffix == ".xlsx"
]


# Read multiple excel files
def concat_fix_sheet(list_of_files):
    # Use list comprehension to read .xlsm files into a list of DataFrames
    dataframes = [
        pd.read_excel(
            file,
            sheet_name="4. Fix Cost (LC) ",
            usecols="A:T",
            skiprows=4,
        )
        for file in list_of_files
    ]

    # Add a new column with filename to each DataFrame
    for i, df in enumerate(dataframes):
        df["source"] = list_of_files[i].stem

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def concat_var_sheet(list_of_files):
    # Use list comprehension to read .xlsm files into a list of DataFrames
    dataframes = [
        pd.read_excel(
            file,
            sheet_name="2. Variable (LC)",
            usecols="A:O",
            skiprows=4,
        )
        for file in list_of_files
    ]

    # Add a new column with filename to each DataFrame
    for i, df in enumerate(dataframes):
        df["source"] = list_of_files[i].stem

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def concat_hc_sheet(list_of_files):
    # Use list comprehension to read .xlsm files into a list of DataFrames
    dataframes = [
        pd.read_excel(
            file,
            sheet_name="6. HC (LC)",
            usecols="A:U",
            skiprows=4,
        )
        for file in list_of_files
    ]

    # Add a new column with filename to each DataFrame
    for i, df in enumerate(dataframes):
        df["source"] = list_of_files[i].stem

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def concat_structure_sheet(list_of_files):
    # Use list comprehension to read .xlsm files into a list of DataFrames
    dataframes = [
        pd.read_excel(
            file,
            sheet_name="1.1 Structural changes (LC)",
            usecols="A:D",
            skiprows=4,
        )
        for file in list_of_files
    ]

    # Add a new column with filename to each DataFrame
    for i, df in enumerate(dataframes):
        df["source"] = list_of_files[i].stem

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    return df


def extract_poc(df):
    # Extract CU / Outlet / Plant information
    return df.assign(source=df["source"].str.extract(r"_([0-9]\w+)")).assign(
        year=lambda row: row["source"].str.split(r"_").str.get(0),
        cu=lambda row: row["source"].str.split(r"_").str.get(1),
        outlet=lambda row: row["source"].str.split(r"_").str.get(2),
        plant=lambda row: row["source"].str.split(r"_").str.get(3),
    )


def reorder_columns(df):
    df = df[
        ["source", "year", "cu", "outlet", "plant"]
        + [
            col
            for col in df.columns
            if col not in ["source", "year", "cu", "outlet", "plant"]
        ]
    ]
    return df


fix = (
    concat_fix_sheet(xls_files)
    .pipe(clean_names)
    .pipe(extract_poc)
    .pipe(reorder_columns)
    .rename(columns={"unnamed_0": "items", "unnamed_19": "comments"})
    .dropna(subset="items")
)
var = (
    concat_var_sheet(xls_files)
    .pipe(clean_names)
    .pipe(extract_poc)
    .pipe(reorder_columns)
    .rename(columns={"unnamed_0": "items", "unnamed_14": "comments"})
    .dropna(subset="items")
)
hc = (
    concat_hc_sheet(xls_files)
    .pipe(clean_names)
    .pipe(extract_poc)
    .pipe(reorder_columns)
    .rename(
        columns={
            "unnamed_0": "items",
            "unnamed_1": "var_fix",
            "unnamed_20": "comments",
        }
    )
    .dropna(subset="items")
)
str = (
    concat_structure_sheet(xls_files)
    .pipe(clean_names)
    .pipe(extract_poc)
    .pipe(reorder_columns)
    .rename(columns={"unnamed_0": "items"})
    .dropna(subset="items")
    .assign(
        Var_Fix=lambda row: row["items"]
        .str.extract(r"(Variable|Fix|Total)")
        .ffill()  # .fillna(method="ffill"),
    )
)

fix.to_csv(path / "output" / "2024 Fix cost.csv", index=False)
var.to_csv(path / "output" / "2024 Var cost.csv", index=False)
hc.to_csv(path / "output" / "2024 Headcount.csv", index=False)
str.to_csv(path / "output" / "2024 Structural changes.csv", index=False)
