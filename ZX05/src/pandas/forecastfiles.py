import pandas as pd


def process_fc_data(path):
    df = read_data(path)
    df = df.pipe(extract_text).pipe(filter_columns).pipe(reorder_columns)
    return df


def read_data(path):
    df = pd.read_csv(path, sep="\t").clean_names()
    df = df.rename(columns={"cost_center": "text_col"})
    return df


def extract_text(df):
    # Extract Cost center and GL accounts using regex
    df = df.assign(
        costctr=df["text_col"].str.extract(
            r"(^[0-9]{4,5}|^IC-.{4,5}|^CY-.{4,5}|^DUMMY_.{3})"
        ),  # ICH-.{4,5}|
        gl_accounts=df["text_col"].str.extract(r"(^K[0-9]+|^S[0-9]+)"),
    )
    df["costctr"] = df["costctr"].str.strip()
    # Fill in missing values for CostCtr
    df["costctr"] = df["costctr"].bfill()  # .fillna(method="backfill")
    return df


def filter_columns(df):
    # filter out missing values
    df = df[~df["gl_accounts"].isna()]
    return df


def reorder_columns(df):
    df = df[
        ["costctr", "gl_accounts"]
        + [
            col
            for col in df.columns
            if col not in ["costctr", "gl_accounts", "text_col"]
        ]
    ]
    return df


def process_fc_numeric_columns(df):
    # Change sign logic, unit in k KRW, Add a new column
    df["fc_tot"] = (
        df["fc_1"]
        + df["fc_2"]
        + df["fc_3"]
        + df["fc_4"]
        + df["fc_5"]
        + df["fc_6"]
        + df["fc_7"]
        + df["fc_8"]
        + df["fc_9"]
        + df["fc_10"]
        + df["fc_11"]
        + df["fc_12"]
    )
    df[
        [
            "fc_1",
            "fc_2",
            "fc_3",
            "fc_4",
            "fc_5",
            "fc_6",
            "fc_7",
            "fc_8",
            "fc_9",
            "fc_10",
            "fc_11",
            "fc_12",
            "fc_tot",
            "plan",
        ]
    ] = (
        df[
            [
                "fc_1",
                "fc_2",
                "fc_3",
                "fc_4",
                "fc_5",
                "fc_6",
                "fc_7",
                "fc_8",
                "fc_9",
                "fc_10",
                "fc_11",
                "fc_12",
                "fc_tot",
                "plan",
            ]
        ].astype(float)
        / -1000
    )
    df["delta_to_plan"] = (df["fc_tot"] - df["plan"]).round(3)
    return df
