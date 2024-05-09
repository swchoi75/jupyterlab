import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def remove_blank_rows(df, numeric_cols):
    # add "check" column that adds the specified numeric columns
    df["check"] = df[numeric_cols].sum(axis="columns")
    # remove blank rows
    df = df[df["check"] != 0]
    # remove the temporary "check" column
    df = df.drop(columns=["check"])
    return df


def main():
    # Filenames
    input_1 = path / "output" / "MYP" / "MYP_2025-2030_v2.csv"
    input_2 = path / "output" / "MYP" / "MYP_2025-2030_plant 9.csv"
    output_file = path / "output" / "MYP" / "MYP_2025-2030_final.csv"

    # Read data
    df_1 = pd.read_csv(input_1)
    df_2 = pd.read_csv(input_2)  # Plant 9 only

    # Process data
    df_1 = df_1[df_1["PL"] != "Plant 9"]  # Remove Plant 9 before allocation
    df = pd.concat([df_1, df_2])  # Add Plant 9 after allocation

    df = df.rename(columns={"source": "country"})
    df["country"] = df["country"].replace(
        {
            "KR": "Korea",
            "IN": "India",
            "JP": "Japan",
            "TH": "Thailand",
        }
    )
    df = df[df["country"] != "AP"]  # Remove AP(=Asia Pacific) data
    df = df[
        ~df["items"].isin(
            ["% of sales", "Reinvestment rate", "CAPEX in % of external Sales"]
        )
    ]

    numeric_cols = [
        "2023",
        "2024",
        "2025",
        "2026",
        "2027",
        "2028",
        "2029",
        "2030",
    ]
    df = df.pipe(remove_blank_rows, numeric_cols)

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
