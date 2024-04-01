import pandas as pd
from janitor import clean_names
from pathlib import Path


# Path
path = Path(__file__).parent

filename = path / "COPA_Sales_2022.csv"
output = path / "COPA_Sales_2022.parquet"


# Read data
df = pd.read_csv(
    filename,
    dtype={
        "Period": str,
        "CoCd": str,
        "Doc. no.": str,
        "Ref.doc.no": str,
        "AC DocumentNo": str,
        "Delivery": str,
        # "Item": int,
        "Plnt": str,
        "Tr.Prt": str,
        "ConsUnit": str,
        "FIRE Plant": str,
        "FIREOutlet": str,
    },
).clean_names()


df.info()


# Write data
df.to_parquet(output)
