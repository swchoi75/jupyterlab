import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def add_excel_table(df, worksheet, worksheet_name):

    # Get the dimensions of the dataframe.
    (max_row, max_col) = df.shape

    # Create a list of column headers, to use in add_table().
    column_settings = [{"header": column} for column in df.columns]

    # Add the Excel table structure.
    worksheet.add_table(
        0,
        0,
        max_row,
        max_col - 1,
        {
            "columns": column_settings,
            "style": "Table Style Medium 3",
            "name": "cc_" + worksheet_name,
        },
    )


def apply_header_format(df, workbook, worksheet):
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
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)


def apply_formatting(workbook, worksheet):

    # Specify row heights
    worksheet.set_row(0, 20)

    # Specify columns widths
    column_list = [
        # category columns
        ("A:A", 9),  # cctr
        ("B:B", 40),  # account_description
        ("C:C", 9),  # cctr
        # value columns
        ("D:T", 10),
    ]
    for col, width in column_list:
        worksheet.set_column(col, width)

    # Number formatting
    format_for_numbers = workbook.add_format({"num_format": "#,##0"})
    worksheet.set_column("D:T", 10, format_for_numbers)

    # Freeze panes
    worksheet.freeze_panes(1, 0)

    # Set zoom
    worksheet.set_zoom(100)

    # You can apply additional formatting to cells as needed


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

            # Add formatting
            apply_formatting(workbook, worksheet)

    print("A file is created")


if __name__ == "__main__":
    main()
