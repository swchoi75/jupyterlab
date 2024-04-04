import pandas as pd
import re
from pathlib import Path
from janitor import clean_names


class Data:
    """Class to handle data processing."""

    def __init__(self):
        self.df = pd.DataFrame()

    def race_df(
        self,
        filename: str,
        sheet_name: str = "Query",
        skiprows: int = 11,
        lc_gc: str = "LC",
    ) -> pd.DataFrame:
        """Reads the excel file, cleans up the dataframe, and adds a currency column.

        Parameters:
        filename (str): The name of the excel file.
        sheet_name (str): The name of the sheet to read from. Defaults to "Query".
        skiprows (int): The number of rows to skip at the beginning. Defaults to 11.
        lc_gc (str): The currency code (e.g., "LC"). Defaults to "LC".

        Returns:
        pd.DataFrame: The cleaned dataframe with the currency column.
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

        # Add a currency column
        df = df.assign(currency=lc_gc)
        df = df[["currency"] + [col for col in df.columns if col != "currency"]]
        return df

    def outlet_df(self, filename: str) -> pd.DataFrame:
        """POC: Plant Outlet Combination"""
        df = pd.read_excel(filename, usecols="A:F", dtype="str")
        df = clean_names(df)
        df = df.drop(columns=["outlet_name"])
        col_poc = ["division", "bu", "new_outlet", "new_outlet_name"]
        df = df[["outlet"] + col_poc]
        return df


class Report:
    """Split P&L and Balance sheet"""

    def __init__(self):
        self.df = pd.DataFrame()

    def profit_and_loss(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_to_drop = ["period_0"] + [f"ytd_{i}" for i in range(13)]
        df = df.drop(cols_to_drop, axis=1)
        df = df.loc[df["financial_statement_item"].str.contains("^3|^CO")]
        return df

    def balance_sheet(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_to_drop = [f"period_{i}" for i in range(13)]
        df = df.drop(cols_to_drop, axis=1)
        df = df.loc[df["financial_statement_item"].str.contains("^1|^2")]
        return df


# Path
path = Path(__file__).parent
input_lc = path / "data" / "Analysis FS Item Hierarchy for CU 698_LC.xlsx"
input_gc = path / "data" / "Analysis FS Item Hierarchy for CU 698_GC.xlsx"
input_meta = path / "meta" / "New outlet.xlsx"
output_pnl = path / "output" / "RACE Profit and Loss.csv"
output_bs = path / "output" / "RACE Balance sheet.csv"


# Create instances
lc = Data()
gc = Data()
meta = Data()
r = Report()


# Read data
df_LC = lc.race_df(input_lc, lc_gc="LC")
df_GC = gc.race_df(input_lc, lc_gc="GC")


# Combine data
df_race = pd.concat([df_LC, df_GC])
df_outlet = meta.outlet_df(input_meta)
race_with_outlet = pd.merge(df_race, df_outlet, on="outlet", how="left")


# Split data
race_pnl = r.profit_and_loss(race_with_outlet)
race_bs = r.balance_sheet(race_with_outlet)


# Output data
race_pnl.to_csv(output_pnl, index=False, na_rep="0")
race_bs.to_csv(output_bs, index=False, na_rep="0")
print("Files are created")
