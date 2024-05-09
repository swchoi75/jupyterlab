import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def main():
    # Filenames
    input_1 = path / "output" / "MYP_2025-2030_v2.csv"
    input_2 = path / "output" / "MYP_2025-2030_plant 9.csv"
    output_file = path / "output" / "MYP_2025-2030_final.csv"

    # Read data
    df_1 = pd.read_csv(input_1)
    df_2 = pd.read_csv(input_2)  # Plant 9 only

    # Process data
    df_1 = df_1[df_1["PL"] != "Plant 9"]  # Remove Plant 9 before allocation
    df = pd.concat([df_1, df_2])  # Add Plant 9 after allocation

    df = df.rename(columns={"source": "country"})
    df = df[df["country"] != "AP"]  # Remove AP(=Asia Pacific) data
    df["country"] = df["country"].replace(
        {
            "KR": "Korea",
            "IN": "India",
            "JP": "Japan",
            "TH": "Thailand",
        }
    )

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
