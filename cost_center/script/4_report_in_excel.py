import pandas as pd
from pathlib import Path
from excel_formatting import (
    add_excel_table,
    apply_header_format,
    apply_conditional_formatting,
    apply_formatting,
)


# Path
path = Path(__file__).parent.parent


# Functions
def main():
    # Filenames
    input_file = path / "output" / "3-2_further_refine_report.csv"
    output_file = path / "output" / "4_cc_report_by_responsible.xlsx"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Write data
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:

        # Unique values
        unique_categories = df["cctr"].unique()

        for category in unique_categories:
            # Create a DataFrame for the current category
            category_df = df[df["cctr"] == category]

            # Write the dataframe data to XlsxWriter. Turn off the default header and
            # index and skip one row to allow us to insert a user defined header.
            category_df.to_excel(
                writer, sheet_name=category, startrow=1, header=False, index=False
            )

            # Access the xlsxwriter workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets[category]

            # Add Excel table
            add_excel_table(category_df, worksheet, category)

            # Add header format
            apply_header_format(category_df, workbook, worksheet)

            # Add conditional formatting
            apply_conditional_formatting(workbook, worksheet)

            # Add various formatting
            apply_formatting(workbook, worksheet)

    print("A file is created")


if __name__ == "__main__":
    main()
