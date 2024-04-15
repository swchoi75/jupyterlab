import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Functions
def concat_sheet(list_of_files, sheet_name, usecols):
    """Define a function to read multiple excel files and concatenate them into a single DataFrame"""

    # Use list comprehension to read .xlsm files into a list of DataFrames
    dataframes = [
        pd.read_excel(
            file,
            sheet_name=sheet_name,
            usecols=usecols,
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
    """Extract CU / Outlet / Plant information"""
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


def main():

    # path
    data_path = path / "data/"
    output_fix = path / "output" / "2024 Fix cost.csv"
    output_var = path / "output" / "2024 Var cost.csv"
    output_hc = path / "output" / "2024 Headcount.csv"
    output_str = path / "output" / "2024 Structural changes.csv"

    # List of multiple excel files
    xls_files = [
        file
        for file in data_path.iterdir()
        if file.is_file() and file.suffix == ".xlsx"
    ]

    # Read data
    df_fix = concat_sheet(xls_files, "4. Fix Cost (LC) ", "A:T")
    df_var = concat_sheet(xls_files, "2. Variable (LC)", "A:O")
    df_hc = concat_sheet(xls_files, "6. HC (LC)", "A:U")
    df_structure = concat_sheet(xls_files, "1.1 Structural changes (LC)", "A:D")

    # Process data
    fix = (
        df_fix.pipe(clean_names)
        .pipe(extract_poc)
        .pipe(reorder_columns)
        .rename(columns={"unnamed_0": "items", "unnamed_19": "comments"})
        .dropna(subset="items")
    )
    var = (
        df_var.pipe(clean_names)
        .pipe(extract_poc)
        .pipe(reorder_columns)
        .rename(columns={"unnamed_0": "items", "unnamed_14": "comments"})
        .dropna(subset="items")
    )
    hc = (
        df_hc.pipe(clean_names)
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
        df_structure.pipe(clean_names)
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

    # Write data
    fix.to_csv(output_fix, index=False)
    var.to_csv(output_var, index=False)
    hc.to_csv(output_hc, index=False)
    str.to_csv(output_str, index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
