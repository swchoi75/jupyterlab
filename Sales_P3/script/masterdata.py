import pandas as pd


def master_cc():
    return pd.read_csv("meta/CC_2023.csv", dtype="str")


def master_gl():
    return pd.read_csv("meta/GL.csv", dtype="str")


def master_ph():
    # Read the PH info CSV file and rename columns
    df = pd.read_csv("meta/PH info.csv", dtype="str")
    df = df.rename(
        columns={
            "Profit Center": "Profit Ctr",
            "Product Hierarchy": "Product Hierarchy",
            "PH_3 simple": "PH_3 simple",
            "PRD/MER": "PRD/MER",
        }
    )

    # Select relevant columns and return the result
    return df[["Profit Ctr", "Product Hierarchy", "PH_3 simple", "PRD/MER"]]


def master_poc():
    return pd.read_csv("../PnL/meta/POC.csv", dtype="str")


def master_cm_cluster():
    return pd.read_excel("meta/YPC1 costing_Icheon.xlsx", engine="openpyxl")


def customer_material():
    # Read Excel file
    df = pd.read_excel(
        "meta/Customer Material.xlsx",
        sheet_name="Sheet1",
        engine="openpyxl",
        dtype=str,
    )

    # Select relevant columns and drop rows with missing values in Customer Material
    df = df[["Product", "Customer Material"]].dropna(
        subset=["Customer Material"])

    return df


mat_0180 = "meta/Material master_0180.xlsx"
mat_2182 = "meta/Material master_2182.xlsx"


def master_material(path):
    # read excel file
    df = pd.read_excel(path, sheet_name="MM", engine="openpyxl", dtype=object)

    # rename columns
    df = df.rename(
        columns={
            "Product Hierachy": "Product Hierarchy",
            "Material": "Product",
            "Profit Center": "Profit Ctr",
        }
    )

    # select relevant columns
    df = df[
        [
            "Product",
            "Profit Ctr",
            "Material type",
            "Product Hierarchy",
            "Standard Price",
            "Ext. Matl. Group",
        ]
    ]

    return df


def master_mat_0180():
    return master_material(mat_0180)


def master_mat_2182():
    return master_material(mat_2182)


def material_master():
    df_0180 = master_mat_0180()
    df_2182 = master_mat_2182()
    return pd.concat([df_0180, df_2182], ignore_index=True)
