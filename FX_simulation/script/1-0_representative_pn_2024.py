import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def main():

    # Filenames
    input_file = path / "data" / "Sales with representative PN.csv"
    output_file = path / "data" / "BOM" / "대표 품번_2024.txt"

    # Read data
    df = pd.read_csv(input_file)

    # Process data
    df = df[df["fy"] == 2024]
    list = df["representative_pn"].sort_values().dropna().unique()

    # Write data
    with open(output_file, "w") as f:
        for item in list:
            f.write(item + "\n")

    print("files are created")


if __name__ == "__main__":
    main()
