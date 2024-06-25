import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions


def main():
    # Filenames
    input_file = path / "output" / "2_fix_act_to_plan_by_month.csv"
    output_file = path / "output" / "3_report.xlsx"

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data

    # Unique values
    unique_categories = df["responsible"].unique()

    # Write data
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        for category in unique_categories:
            # Create a DataFrame for the current category
            category_df = df[df["responsible"] == category]
            category_df = category_df.drop(columns=["responsible"])

            # Write the dataframe data to XlsxWriter. Turn off the default header and
            # index and skip one row to allow us to insert a user defined header.
            category_df.to_excel(
                writer, sheet_name=category, startrow=1, header=False, index=False
            )

            # Access the xlsxwriter workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets[category]

            # Get the dimensions of the dataframe.
            (max_row, max_col) = category_df.shape

            # Create a list of column headers, to use in add_table().
            column_settings = [{"header": column} for column in category_df.columns]

            # Add the Excel table structure.
            worksheet.add_table(
                0,
                0,
                max_row,
                max_col - 1,
                {
                    "columns": column_settings,
                    "style": "Table Style Medium 3",
                    "name": category,
                },
            )

            # Add header format
            header_format = workbook.add_format(
                {
                    "bold": True,
                    "align": "left",
                    "font_size": "14",
                    "font_color": "#4B4B46",  # VT Gray
                    "bg_color": "#F0E614",  # VT Yellow
                }
            )

            # Write the header row explicitly with your formatting
            for col_num, value in enumerate(category_df.columns.values):
                worksheet.write(0, col_num, value, header_format)

            # format_excel_table(workbook, worksheet)

    print("A file is created")


if __name__ == "__main__":
    main()
