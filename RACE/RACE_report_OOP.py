import pandas as pd
import re
from pathlib import Path
from janitor import clean_names


class Data:
    """Class to handle data processing."""

    def race_df(
        self, filename: str, sheet_name: str = "Query", skiprows: int = 11
    ) -> pd.DataFrame:
        """Reads the excel file and cleans up the dataframe.

        Parameters:
        filename (str): The name of the excel file.
        sheet_name (str): The name of the sheet to read from. Defaults to "Query".
        skiprows (int): The number of rows to skip at the beginning. Defaults to 11.

        Returns:
        pd.DataFrame: The cleaned dataframe.
        """
        df = pd.read_excel(filename, sheet_name=sheet_name, skiprows=skiprows)
        df = df.astype({"ConsUnit": str, "Plant": str, "Outlet": str})
        df = df.rename(
            columns={
                "Unnamed: 1": "FS item description",
                "Unnamed: 3": "CU name",
                "Unnamed: 5": "Plant name",
                "Unnamed: 7": "Outlet name",
                "YTD - 1": "YTD PM",
            }
        )
        df = df.rename(columns=lambda x: re.sub("\nACT", "", x))
        df = clean_names(df)
        return df

    def process_currency(self, df, lc_gc):
        """Adds a currency column to the dataframe."""
        df = df.assign(currency=lc_gc)
        df = df[["currency"] + [col for col in df.columns if col != "currency"]]
        return df

    def outlet_df(self, filename):
        """POC: Plant Outlet Combination"""
        col_poc = ["division", "bu", "new_outlet", "new_outlet_name"]

        df = pd.read_excel(filename, usecols="A:F", dtype="str")
        df = clean_names(df)
        df = df.drop(columns=["outlet_name"])
        df = df[["outlet"] + col_poc]
        return df


class Report:
    """Split P&L and Balance sheet"""

    def profit_and_loss(self, df):
        cols_to_drop = ["period_0"] + [f"ytd_{i}" for i in range(13)]
        df = df.drop(cols_to_drop, axis=1)
        df = df.loc[df["financial_statement_item"].str.contains("^3|^CO")]
        return df

    def balance_sheet(self, df):
        cols_to_drop = [f"period_{i}" for i in range(13)]
        df = df.drop(cols_to_drop, axis=1)
        df = df.loc[df["financial_statement_item"].str.contains("^1|^2")]
        return df


def process_data(input_file: str, currency: str) -> pd.DataFrame:
    data = Data()
    df = data.race_df(input_file)
    df = data.process_currency(df, currency)
    return df


# Path
path = Path(__file__).parent
input_lc = path / "data" / "Analysis FS Item Hierarchy for CU 698_LC.xlsx"
input_gc = path / "data" / "Analysis FS Item Hierarchy for CU 698_GC.xlsx"
input_meta = path / "meta" / "New outlet.xlsx"
output_pnl = path / "output" / "RACE Profit and Loss.csv"
output_bs = path / "output" / "RACE Balance sheet.csv"


# Read data
try:
    lc = process_data(str(input_lc), "LC")
    gc = process_data(str(input_gc), "GC")
except FileNotFoundError:
    print("One of the input files was not found.")


# Combine data
race = pd.concat([lc, gc])
outlet = Data().outlet_df(input_meta)
race_with_outlet = pd.merge(race, outlet, on="outlet", how="left")


# Split data
race_pnl = Report().profit_and_loss(race_with_outlet)
race_bs = Report().balance_sheet(race_with_outlet)


# Output data
race_pnl.to_csv(output_pnl, index=False, na_rep="0")
race_bs.to_csv(output_bs, index=False, na_rep="0")
print("Files are created")
