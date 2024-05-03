import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def main():
    # Filenames
    input_1 = path / "output" / "BP_2025+9 Division E.csv"
    input_2 = path / "output" / "BP_2024+9 Division P.csv"
    input_3 = path / "output" / "BP_2025+9 Division P.csv"
    output_file = path / "output" / "BP_2025+9 Division E and P.csv"

    # Read data
    df_1 = pd.read_csv(input_1)
    df_2 = pd.read_csv(input_2)
    df_3 = pd.read_csv(input_3)

    # Process data
    df = pd.concat([df_1, df_2, df_3])

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
