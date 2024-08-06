import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent.parent.parent


# Functions
def process_ke30(df):
    # select columns
    selected_columns = [
        "period",
        "profit_center",
        "product",
        "quantity",
        "stock_val",
        "revenues",
    ]
    df = df[selected_columns]

    # text slicing & convert data type
    df["period"] = df["period"].str[-2:]  # extract the last two characters
    df["period"] = df["period"].astype(int)

    # add column
    df["source"] = "KE30"

    # reorder columns
    df = df[["source"] + [col for col in df.columns if col not in ["source"]]]

    return df


def process_gl(df):
    # select columns
    selected_columns = [
        "period",
        "profit_ctr",
        "material",
        "quantity",
        "amount_in_local_cur",
    ]
    df = df[selected_columns]

    # rename columns
    df = df.rename(
        columns={
            "profit_ctr": "profit_center",
            "material": "product",
            "amount_in_local_cur": "stock_val",
        }
    )

    # add column
    df["source"] = "GL"

    # reorder columns
    df = df[["source"] + [col for col in df.columns if col not in ["source"]]]

    return df


def summary_data(df):
    df = (
        df.groupby(
            [
                col
                for col in df.columns
                if col not in ["quantity", "revenues", "stock_val"]
            ]
        )
        .agg(
            qty=("quantity", "sum"),
            sales=("revenues", "sum"),
            costs=("stock_val", "sum"),
        )
        .reset_index()
    )
    return df


def main():
    # Variables
    year = "2024"

    # Filenames
    input_1 = path / "db" / f"KE30_{year}.csv"
    input_2 = path / "db" / f"GL_{year}.csv"
    output_file = path / "output" / f"KE30 and GL Account_{year}.csv"

    # Read data
    df_ke30 = pd.read_csv(
        input_1,
        dtype={
            # text
            "period": str,
            # number
            "revenues": int,
            "stock_val": int,
            "quantity": int,
        },
    )
    df_gl = pd.read_csv(
        input_2,
        dtype={
            # number
            "quantity": int,
            "amount_in_doc_curr": int,
            "amount_in_local_cur": int,
        },
    )

    # Process data
    df_ke30 = process_ke30(df_ke30)
    df_gl = process_gl(df_gl)

    df = pd.concat([df_ke30, df_gl])
    df["revenues"] = df["revenues"].fillna(0).astype(int)

    df = df.pipe(summary_data).sort_values(
        by=["source", "period", "profit_center", "product"],
        ascending=[False, True, True, True],
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
