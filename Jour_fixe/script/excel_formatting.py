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
            "name": worksheet_name,
        },
    )


def apply_header_formatting(df, workbook, worksheet):

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


def apply_other_formatting(workbook, worksheet):

    # Specify row heights
    worksheet.set_row(0, 20)

    # Specify columns widths
    column_list = [
        ("C:C", 50),  # Title
        ("D:D", 100),  # Description
        ("E:F", 11),  # Status, Due_date
    ]
    for col, width in column_list:
        worksheet.set_column(col, width)

    # Enable text wrapping for the specific column
    column_format = workbook.add_format()
    column_format.set_text_wrap()
    worksheet.set_column("C:C", 50, column_format)  # Title

    # Freeze panes
    worksheet.freeze_panes(1, 0)

    # Set zoom
    worksheet.set_zoom(100)

    # You can apply additional formatting to cells as needed
