# Functions


def add_label(workbook, worksheet, year, month, responsible_name):
    # Format
    cell_format = workbook.add_format(
        {"bold": True, "bg_color": "#EBF1DE"}  # light green
    )

    # Write data
    worksheet.write("B2", f"CC responsible: {responsible_name}", cell_format)
    worksheet.write("B3", f"Year: {year}", cell_format)
    worksheet.write("B4", f"YTD month: {month:0>2}", cell_format)  # ensure 2 digits
    worksheet.write("B5", "Scope: primary fix costs", cell_format)
    worksheet.write("T5", "Unit: T KRW", cell_format)


def add_excel_table(df, worksheet, worksheet_name, skiprows):

    # Get the dimensions of the dataframe.
    (max_row, max_col) = df.shape

    # Create a list of column headers, to use in add_table().
    column_settings = [{"header": column} for column in df.columns]

    # Add the Excel table structure.
    worksheet.add_table(
        0 + skiprows,
        0,
        max_row + skiprows,
        max_col - 1,
        {
            "columns": column_settings,
            "style": "None",
            # "style": "Table Style Medium 3",
            "name": "cc_" + worksheet_name,
        },
    )


def apply_header_formatting(df, workbook, worksheet, skiprows):
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
        worksheet.write(0 + skiprows, col_num, value, format_header)


def apply_conditional_formatting(workbook, worksheet):
    # Conditional Formatting
    format_bold = workbook.add_format({"bold": True})
    worksheet.conditional_format(
        "A2:X500",
        {
            "type": "formula",
            "criteria": '=ISNUMBER(SEARCH("subtotal",$B2))',
            "format": format_bold,
        },
    )

    format_bold_and_color = workbook.add_format({"bold": True, "bg_color": "yellow"})
    worksheet.conditional_format(
        "A2:X500",
        {
            "type": "formula",
            "criteria": '=ISNUMBER(SEARCH("299 Total Labor Costs",$B2))',
            "format": format_bold_and_color,
        },
    )
    worksheet.conditional_format(
        "A2:X500",
        {
            "type": "formula",
            "criteria": '=ISNUMBER(SEARCH("465 Cost of materials",$B2))',
            "format": format_bold_and_color,
        },
    )


def delta_conditional_formatting(workbook, worksheet):
    # Conditional Formatting
    format_bold_and_color = workbook.add_format({"font_color": "red"})
    worksheet.conditional_format(
        "V2:V500",
        {
            "type": "cell",
            "criteria": "<",
            "value": 0,
            "format": format_bold_and_color,
        },
    )
    worksheet.conditional_format(
        "A2:B500",
        {
            "type": "formula",
            "criteria": "=$V2<0",
            "format": format_bold_and_color,
        },
    )


def grand_total_conditional_formatting(workbook, worksheet):
    # Conditional Formatting
    format_bold_and_color = workbook.add_format({"bold": True, "bg_color": "yellow"})
    worksheet.conditional_format(
        "A2:X500",
        {
            "type": "formula",
            "criteria": '=ISNUMBER(SEARCH("subtotal",$A2))',
            "format": format_bold_and_color,
        },
    )


def apply_other_formatting(workbook, worksheet, skiprows):

    # Specify row heights
    worksheet.set_row(0 + skiprows, 20)

    # Specify columns widths
    column_list = [
        # category columns
        ("B:B", 46),  # account_description
        ("G:G", 14),  # plant_name
        # value columns
        ("H:S", 9),
        ("T:X", 10),
    ]
    for col, width in column_list:
        worksheet.set_column(col, width)

    # Number formatting
    format_numbers = workbook.add_format({"num_format": "#,##0"})
    worksheet.set_column("H:S", 9, format_numbers)
    worksheet.set_column("T:X", 10, format_numbers)

    # Freeze panes
    worksheet.freeze_panes(1 + skiprows, 0)

    # Set zoom
    worksheet.set_zoom(100)

    # You can apply additional formatting to cells as needed


def add_headcount_sheet(
    writer, df, sheet_name, year, month, responsible_name, skiprows
):

    # Access the xlsxwriter workbook and worksheet
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    # Add label
    add_label(workbook, worksheet, year, month, responsible_name)

    # Add Excel table
    add_excel_table(df, worksheet, sheet_name, skiprows)

    # Add header formatting
    apply_header_formatting(df, workbook, worksheet, skiprows)

    # Add conditional formatting
    delta_conditional_formatting(workbook, worksheet)
    grand_total_conditional_formatting(workbook, worksheet)

    # Add various other formatting
    apply_other_formatting(workbook, worksheet, skiprows)

    # Add worksheet tab color
    worksheet.set_tab_color("yellow")


def add_summary_sheet(writer, df, sheet_name, year, month, responsible_name, skiprows):

    # Access the xlsxwriter workbook and worksheet
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    # Add label
    add_label(workbook, worksheet, year, month, responsible_name)

    # Add Excel table
    add_excel_table(df, worksheet, sheet_name, skiprows)

    # Add header formatting
    apply_header_formatting(df, workbook, worksheet, skiprows)

    # Add conditional formatting
    delta_conditional_formatting(workbook, worksheet)
    grand_total_conditional_formatting(workbook, worksheet)

    # Add various other formatting
    apply_other_formatting(workbook, worksheet, skiprows)

    # Add worksheet tab color
    worksheet.set_tab_color("yellow")


def add_cc_sheet(writer, df, sheet_name, year, month, responsible_name, skiprows):
    # Access the xlsxwriter workbook and worksheet
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    # Add label
    add_label(workbook, worksheet, year, month, responsible_name)

    # Add Excel table
    add_excel_table(df, worksheet, sheet_name, skiprows)

    # Add header formatting
    apply_header_formatting(df, workbook, worksheet, skiprows)

    # Add conditional formatting
    apply_conditional_formatting(workbook, worksheet)
    delta_conditional_formatting(workbook, worksheet)

    # Add various other formatting
    apply_other_formatting(workbook, worksheet, skiprows)
