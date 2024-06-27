import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def format_excel_table(workbook, worksheet):
    # Specify row heights
    worksheet.set_row(0, 20)

    # Specify key columns widths
    worksheet.set_column("C:C", 26)  # account_name
    worksheet.set_column("D:D", 9)  # pctr
    worksheet.set_column("F:F", 40)  # acc_lv1
    worksheet.set_column("G:I", 22)  # acc_lv2, acc_lv3, responsible

    # Specify values columns widths and format
    worksheet.set_column("J:Z", 10)

    # Enable text wrapping for an entire column
    column_format = workbook.add_format()
    column_format.set_text_wrap()

    # Freeze panes
    worksheet.freeze_panes(1, 0)

    # Set zoom
    worksheet.set_zoom(100)

    # You can apply additional formatting to cells as needed


def main():
    # Filenames
    input_file = path / "output" / "3-2_further_refine_report.csv"
    output_file = path / "output" / "4_report.xlsx"

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

            # Get the dimensions of the dataframe.
            (max_row, max_col) = category_df.shape

            # Create a list of column headers, to use in add_table().
            header_columns = [{"header": column} for column in category_df.columns]

            # Add the Excel table structure.
            worksheet.add_table(
                0,
                0,
                max_row,
                max_col - 1,
                {
                    "columns": header_columns,
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

            format_excel_table(workbook, worksheet)

    print("A file is created")


if __name__ == "__main__":
    main()
