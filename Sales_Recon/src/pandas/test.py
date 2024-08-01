import pandas as pd
from janitor import clean_names
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Functions


input_file = path / "data" / "VAN CM" / "result.csv"
output_file = path / "data" / "VAN CM" / "result_2.csv"

df = pd.read_csv(input_file).clean_names(strip_accents=False)


df.head().to_csv(output_file, index=False, encoding="utf-8-sig")
