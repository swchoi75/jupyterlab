import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Filenames
input_file = path / "output" / "Sales BU CT_with PH.csv"
output_file = path / "output" / "Sales high runner per PH.csv"
result_file = path / "output" / "Sales high runner PN.csv"


# Read data
df = pd.read_csv(input_file)


# Aggregate data
df = (
    df.groupby(
        [
            "fy",
            "recordtype",
            "product_hierarchy",
            "PH_description",
            "product",
            "material_type",
            "ext_matl_group",
            "product_group",
            "productline",
            "customer_group",
            "HMG_PN",
        ],
        dropna=False,  # To avoid deleting rows with missing values
    )
    .sum(
        [
            "quantity",
            "totsaleslc",
        ]
    )
    .reset_index()
)


# Filter data
df = df.loc[df["recordtype"] == "F"]
df = df.loc[(df["material_type"] == "FERT") | (df["material_type"] == "HALB")]
df = df.loc[(df["totsaleslc"] > 5e7) | (df["totsaleslc"] < -5e7)]  # over +/- 50M KRW


# Sort data
df = df.sort_values(
    by=["fy", "product_hierarchy", "totsaleslc"], ascending=[True, True, False]
)


# Filter the first occurrence of each category
result = df.groupby(["fy", "product_hierarchy"]).head(1)


# Write data
df.to_csv(output_file, index=False)
result.to_csv(result_file, index=False)
print("files are created.")
