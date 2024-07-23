import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent


def main():
    # Filenames
    input_file = path / "output" / "Sales Div E.csv"
    mm_file = path / "meta" / "material_master_Div E.xlsx"
    cg_file = path / "meta" / "customer_group.xlsx"
    ph_file = path / "meta" / "product_hierarchy_Div E.xlsx"
    pg_file = path / "meta" / "product_group_Div E.xlsx"
    output_file = path / "output" / "Sales Div E_with master data.csv"

    # Read data
    df = pd.read_csv(input_file)
    cg = pd.read_excel(cg_file, usecols="A:B")
    mm = pd.read_excel(mm_file, usecols="A:H").clean_names()
    mm = mm[["material", "material_type", "ext_matl_group", "product_hierachy"]]
    mm = mm.rename(columns={"product_hierachy": "product_hierarchy"})
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
    df["product_group"] = df["product_hierarchy"].str[2:5].str.extract(r"([A-Z]\d{2})")

    # Add productline out of product_group
    df = df.merge(pg, how="left", on="product_group")

    # Extract HMG customer part number
    pattern = r"\b([A-Z0-9]{5}-[A-Z0-9]{5})\b"
    df["HMG_PN"] = df["matnr_descr"].str.extract(pattern)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created.")


if __name__ == "__main__":
    main()
