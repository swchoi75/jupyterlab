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
            "style": "None",
            # "style": "Table Style Medium 3",
            "name": "cc_" + worksheet_name,
        },
    )


def apply_header_format(df, workbook, worksheet):
    # Add header format
    format_header = workbook.add_format(
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
        worksheet.write(0, col_num, value, format_header)


def apply_conditional_formatting(workbook, worksheet):
    # Conditional Formatting
    format_bold = workbook.add_format({"bold": True})
    worksheet.conditional_format(
        "A2:T500",
        {
            "type": "formula",
            "criteria": '=ISNUMBER(SEARCH("subtotal",$B2))',
            "format": format_bold,
        },
    )

    format_bold_and_color = workbook.add_format({"bold": True, "bg_color": "yellow"})
    worksheet.conditional_format(
        "A2:T500",
        {
            "type": "formula",
            "criteria": '=ISNUMBER(SEARCH("299 Total Labor Costs",$B2))',
            "format": format_bold_and_color,
        },
    )
    worksheet.conditional_format(
        "A2:T500",
        {
            "type": "formula",
            "criteria": '=ISNUMBER(SEARCH("465 Cost of materials",$B2))',
            "format": format_bold_and_color,
        },
    )


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
        ("D:O", 11),
        ("P:T", 12),
    ]
    for col, width in column_list:
        worksheet.set_column(col, width)

    # Number formatting
    format_numbers = workbook.add_format({"num_format": "#,##0"})
    worksheet.set_column("D:O", 11, format_numbers)
    worksheet.set_column("P:T", 12, format_numbers)

    # Freeze panes
    worksheet.freeze_panes(1, 0)

    # Set zoom
    worksheet.set_zoom(100)

    # You can apply additional formatting to cells as needed
