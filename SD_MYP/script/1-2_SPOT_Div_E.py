import pandas as pd
from pathlib import Path
from common_function import clean_column_names


# Path
path = Path(__file__).parent.parent


# Functions
def main():
    # Filenames
    input_file = (
        path
        / "data"
        / "DIV E_Sales Early View 2025+9_status_24.04.2024_Legal view (AP).xlsx"
    )
    output_file = path / "output" / "SPOT" / "BP_2025+9 Division E.csv"

    # Read data
    df = pd.read_excel(input_file, sheet_name="Database", skiprows=4)

    # Process data
    df = df.pipe(clean_column_names)
    df = df.assign(division="DIV E")  # add column "division"

    # Write data
    df.to_csv(output_file, index=False)
    print("A file is created")


if __name__ == "__main__":
    main()
