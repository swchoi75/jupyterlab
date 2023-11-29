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
input_file = path / "output" / "Sales BU CT.csv"
mm_file = path / "meta" / "material_master_BU CT.xlsx"
cg_file = path / "meta" / "customer_group.xlsx"
ph_file = path / "meta" / "product_hierarchy_BU CT.xlsx"
pg_file = path / "meta" / "product_group_BU CT.xlsx"
output_file = path / "output" / "Sales BU CT_with meta.csv"


# Read data
df = pd.read_csv(input_file)
cg = pd.read_excel(cg_file, usecols="A:B")
mm = pd.read_excel(mm_file, usecols="A:H").clean_names()
mm = mm[["material", "material_type","ext_matl_group", "product_hierachy"]]
mm = mm.rename(columns={"product_hierachy":"product_hierarchy"})
ph = pd.read_excel(ph_file, usecols="A:D")
pg = pd.read_excel(pg_file, usecols="A:B")


# Add customer_group
df = df.merge(cg, how="left", on="sold_to_name_1")

              
# Add material_type & product hierarchy
df = df.merge(mm, how="left", left_on="product", right_on="material")
df = df.drop(columns=["material"])


# Add PH description
df = df.merge(ph, how="left", on="product_hierarchy")


# Apply string slicing to get product_group
df['product_group'] = df['product_hierarchy'].str[2:5].str.extract(r'([A-Z]\d{2})')


# Add productline out of product_group
df = df.merge(pg, how="left", on="product_group")


# Extract HMG customer part number
pattern = r'\b([A-Z0-9]{5}-[A-Z0-9]{5})\b'
df["HMG_PN"] = df["matnr_descr_"].str.extract(pattern)


# Write data
df.to_csv(output_file, index=False)
print("A file is created.")
