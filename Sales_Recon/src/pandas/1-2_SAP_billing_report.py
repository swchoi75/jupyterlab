import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def select_columns(df, selected_columns):
    df = df[selected_columns]
    return df


def remove_columns(df, cols_to_remove):
    df = df[[col for col in df.columns if col not in cols_to_remove]]
    return df


def get_latest_price(df):
    # Combine 'customer' and 'material' columns into a new column 'customer_material'
    df["customer_material"] = df["customer"] + "_" + df["material"]

    # Remove duplicates based on 'customer_material' column
    df = df[~df.duplicated(subset="customer_material", keep="first")]

    return df


def process_mobis(df):
    # Special dealing for Customer MOBIS
    df["고객명"] = df.apply(
        lambda row: (
            "MOBIS module"
            if row["고객명"] == "MOBIS"
            and row["ship_to_party"] in ["10003814", "10046779", "40053559", "40043038"]
            else "MOBIS AS" if row["고객명"] == "MOBIS" else row["고객명"]
        ),
        axis=1,
    )
    return df


def process_customer_pn(df):
    # Remove hyphen(-), blank space, and underscore(_) in Customer Part Number
    df["customer_pn"] = df["customer_part_number"].str.replace("-", "", regex=True)
    df["customer_pn"] = df["customer_pn"].str.replace(" ", "", regex=True)
    df["customer_pn"] = df["customer_pn"].str.replace("_", "", regex=True)
    return df


def extract_final_customer_pn(df):
    # Extract final customer PN as temp from Material Description
    df["temp"] = df["material_description"].str.extract(r"([a-zA-Z0-9-]*)$")
    df["temp"] = df["temp"].str.replace("-", "", regex=True)
    return df


def process_gm_mobis(df):
    # Special dealing for customer GM and MOBIS module
    df["customer_pn_rev1"] = df["customer_pn"].copy()
    df.loc[df["고객명"] == "한국지엠", "customer_pn_rev1"] = df.loc[
        df["고객명"] == "한국지엠", "customer_pn"
    ].str.replace("P", "")
    df.loc[df["고객명"] == "MOBIS module", "customer_pn_rev1"] = df.loc[
        df["고객명"] == "MOBIS module", "temp"
    ]
    df.loc[df["customer_pn"] == "392502", "customer_pn_rev1"] = "392502E000"
    return df


def process_inverter(df):
    # Special dealing for Inverter & others from MOBIS module
    conditions = [
        "HIT100",
        "ME",
        "MV",
        "TSD",
        "cover",
        "NA",
        "V01",
        "V02",
        "V03",
        "366B02B",
    ]
    df["customer_pn_rev"] = df.apply(
        lambda row: (
            row["customer_pn"]
            if row["customer_pn_rev1"] in conditions
            else row["customer_pn_rev1"]
        ),
        axis=1,
    )
    df.drop(columns=["customer_pn_rev1"], inplace=True)
    return df


def summary_data(df):
    key_columns = [
        "plant",
        "고객명",
        "sold_to_party",
        "customer_name",
        "customer_pn_rev",
        "customer_pn",
        "material_number",
        "division",
        "profit_center",
        "material_description",
        "customer_material",
        "current_price",
        "cn_ty",
        "curr",
    ]
    agg_dict = {
        "billing_quantity": "sum",
        "sales_amount_krw": "sum",
    }
    df = (
        df.groupby(key_columns, dropna=False)
        .agg(agg_dict)
        .reset_index()
        .rename(columns={"billing_quantity": "qty", "sales_amount_krw": "amt"})
    )

    # Calculate avg_billing_price
    df["avg_billing_price"] = round(df["amt"] / df["qty"], 2)

    return df


def main():

    # Variables
    from common_variable import year, month, day

    # Filenames
    input_1 = path / "data" / "SAP" / f"Billing_0180_{year}_{month}.xlsx"
    input_2 = path / "data" / "SAP" / f"Billing_2182_{year}_{month}.xlsx"
    input_3 = path / "data" / "SAP" / f"Price_{year}-{month}-{day}.xls"

    meta_1 = path / "meta" / "고객명.csv"
    meta_2 = path / "meta" / "공장명.csv"

    output_1 = path / "output" / "1-2. SAP billing summary_test.csv"
    output_2 = path / "output" / "1-2. price_latest_test.csv"
    output_3 = path / "output" / "1-2. SAP billing details_test.csv"

    # Read data
    df_0180 = (
        pd.read_excel(input_1)
        .clean_names(strip_accents=False, strip_underscores=True, case_type="snake")
        .dropna(subset=["sales_organization"])
    )

    df_2182 = (
        pd.read_excel(input_2)
        .clean_names(strip_accents=False, strip_underscores=True, case_type="snake")
        .dropna(subset=["sales_organization"])
    )

    df_price = (
        pd.read_csv(
            input_3,
            skiprows=3,
            sep="\t",
            encoding="UTF-16LE",
            skipinitialspace=True,
            thousands=",",
            engine="python",
        )
        .clean_names(strip_accents=False, strip_underscores=True, case_type="snake")
        .dropna(subset=["s_org"])
        .pipe(remove_columns, ["unnamed_0", "re_st"])
        .rename(
            columns={
                "unit": "curr",
                "unit_1": "per_unit",
                "amount": "current_price",
            }
        )
    )

    df_customer = (
        pd.read_csv(meta_1)
        .clean_names(strip_accents=False, strip_underscores=True, case_type="snake")
        .pipe(select_columns, ["sold_to_party", "고객명"])
    )

    df_customer_plant = (
        pd.read_csv(meta_2, dtype=str)
        .clean_names(strip_accents=False, strip_underscores=True, case_type="snake")
        .pipe(select_columns, ["ship_to_party", "공장명"])
    )

    # Process data
    df_0180["plant"] = "0180"
    df_2182["plant"] = "2182"
    df = pd.concat([df_0180, df_2182])

    df_price_latest = get_latest_price(df_price)

    ## Join dataframes
    df = (
        df.merge(
            df_price_latest,
            left_on=["sold_to_party", "material_number"],
            right_on=["customer", "material"],
            how="left",
        )
        .drop(columns=["customer", "material"])
        .merge(df_customer, on="sold_to_party", how="left")
        .merge(df_customer_plant, on="ship_to_party", how="left")
    )

    df = (
        df.pipe(process_mobis)
        .pipe(process_customer_pn)
        .pipe(extract_final_customer_pn)
        .pipe(process_gm_mobis)
        .pipe(process_inverter)
    )

    df_summary = summary_data(df)

    # Write data
    df_summary.to_csv(output_1, index=False, encoding="utf-8-sig")  # for 한글 표시
    df_price_latest.to_csv(output_2, index=False)
    df.to_csv(output_3, index=False, encoding="utf-8-sig")  # for 한글 표시
    print("Files are created")


if __name__ == "__main__":
    main()
