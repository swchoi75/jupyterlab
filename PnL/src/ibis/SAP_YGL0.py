import pandas as pd
import ibis
from ibis import selectors as s
from ibis import _

# ibis.options.interactive = True
from pathlib import Path


# Path
try:
    path = Path(__file__).parent.parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent.parent


# Functions
def read_multiple_files(list_of_files):
    dataframes = [
        pd.read_csv(
            file,
            sep="\t",
        )
        for file in list_of_files
    ]

    # Add a new column with filename to each DataFrame
    for i, df in enumerate(dataframes):
        df["source"] = list_of_files[i].stem

    # Merge the list of DataFrames into a single DataFrame
    df = pd.concat(dataframes)

    # reorder columns
    df = df[["source"] + [col for col in df.columns if col not in ["source"]]]

    return df


def change_column_names(df):
    return df.rename(
        columns={
            # "source": "PrCr",
            "OneGL B/S + P/L": "OneGL",
            "01": "Jan",
            "02": "Feb",
            "03": "Mar",
            "04": "Apr",
            "05": "May",
            "06": "Jun",
            "07": "Jul",
            "08": "Aug",
            "09": "Sep",
            "10": "Oct",
            "11": "Nov",
            "12": "Dec",
        }
    )


def select_columns(tbl):
    tbl = tbl.select(
        "source",
        "OneGL",
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    )
    return tbl


def main():

    # Filenames
    data_path = path / "data" / "SAP YGL0"
    dat_files = [
        file for file in data_path.iterdir() if file.is_file() and file.suffix == ".dat"
    ]
    meta_file = path / "meta" / "Lookup_table.csv"
    output_file = path / "output" / "SAP YGL0 P&L.csv"

    # Read data
    df = read_multiple_files(dat_files)
    df = change_column_names(df)
    df_lookup = pd.read_csv(meta_file, dtype=str)

    # Convert dataframe to ibis table
    tbl = ibis.memtable(df, name="tbl")
    tbl_lookup = ibis.memtable(df_lookup, name="tbl_lookup")

    # Process data
    tbl = (
        tbl.pipe(select_columns)
        .mutate(source=_.source.replace(".dat", ""))
        .mutate(Key=_.OneGL.re_extract(r"([0-9]+|^K[0-9]+|^P[0-9]+)", 1))
        .mutate(s.across(s.numeric(), _ / -1000))
        .rename({"PrCr": "source"})
    )

    tbl_lookup = tbl_lookup.mutate(
        Key=_.Key.re_extract(r"([0-9]+|^K[0-9]+|^P[0-9]+)", 1)
    )

    tbl = tbl.join(tbl_lookup, "Key", how="inner")
    tbl = tbl.select(["PrCr", "A", "B", "C", "D"], s.numeric(), ["Key", "OneGL"])

    # Write data
    tbl.to_pandas().to_csv(output_file, index=False)
    print("A files is created")


if __name__ == "__main__":
    main()
