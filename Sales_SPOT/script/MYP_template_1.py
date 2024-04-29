import csv
import xlsxwriter
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def add_text(worksheet):
    worksheet.write("C6", "Instruction / comments")


def add_items(worksheet, list_of_data):
    # Start from the cell. Rows and columns are zero indexed.
    row = 0
    col = 3

    # Write the data from the CSV file, handling empty lines
    for data in list_of_data:
        # Check if the row is not empty before accessing data[0]
        if data:
            worksheet.write(row, col, data[1])  # items
        row += 1


def add_poc(worksheet, list_of_data):
    # Start from the cell. Rows and columns are zero indexed.
    row = 1
    col = 4

    for data in list_of_data:
        worksheet.write(row, col, data[0])  # Division
        col += 9

    # Start from the cell. Rows and columns are zero indexed.
    row = 2
    col = 4

    for data in list_of_data:
        worksheet.write(row, col, data[1])  # BU
        col += 9

    # Start from the cell. Rows and columns are zero indexed.
    row = 3
    col = 4

    for data in list_of_data:
        worksheet.write(row, col, data[2])  # Productlines
        col += 9


def add_years(worksheet, list_of_data):
    # Start from the cell. Rows and columns are zero indexed.
    rows = [5, 47, 76, 104, 122, 134, 143]

    for row in rows:
        col = 4
        for data in list_of_data:
            worksheet.write(row, col, data)
            col += 1


def add_zero(worksheet, list_of_data):
    # Start from the cell. Rows and columns are zero indexed.
    rows = (
        list(range(7, 45))
        + list(range(105, 113))
        + list(range(123, 130))
        + list(range(135, 138))
        + list(range(144, 147))
    )

    for row in rows:
        col = 4
        for data in list_of_data:
            worksheet.write(row, col, data)
            col += 1


def add_formula_vt(worksheet):
    # Dynamic array
    worksheet.write_dynamic_array_formula("E50:L50", "=E8:L8")  # Sales
    worksheet.write_dynamic_array_formula("E51:L51", "=E9:L9")  # Therein ICO Sales
    worksheet.write_dynamic_array_formula("E52:L52", "=E15:L15")  # Sales variations
    worksheet.write_dynamic_array_formula("E53:L53", "=E8:L8+E10:L10+E12:L12+E13:L13")
    worksheet.write_dynamic_array_formula(
        "E54:L54", "=IF(ISERROR(E53:L53/E50:L50),0,(E53:L53/E50:L50))"
    )  # % of sales
    worksheet.write_dynamic_array_formula("E55:L55", "=E53:L53+E16:L16")
    worksheet.write_dynamic_array_formula(
        "E56:L56", "=IF(ISERROR(E55:L55/E50:L50),0,(E55:L55/E50:L50))"
    )  # % of sales
    worksheet.write_dynamic_array_formula("E57:L57", "=E22:L22+E30:L30")
    worksheet.write_dynamic_array_formula("E58:L58", "=E28:L28+E29:L29")
    worksheet.write_dynamic_array_formula("E59:L59", "=E31:L31")
    worksheet.write_dynamic_array_formula("E60:L60", "=E55:L55+E57:L57+E59:L59")
    worksheet.write_dynamic_array_formula(
        "E61:L61", "=IF(ISERROR(E60:L60/E50:L50),0,(E60:L60/E50:L50))"
    )  # % of sales
    worksheet.write_dynamic_array_formula("E62:L62", "=E32:L32+E33:L33+E34:L34+E35:L35")
    worksheet.write_dynamic_array_formula("E63:L63", "=E33:L33+E34:L34+E35:L35")
    worksheet.write_dynamic_array_formula("E64:L64", "=E36:L36+E37:L37+E38:L38")
    worksheet.write_dynamic_array_formula("E65:L65", "=E37:L37+E38:L38")
    worksheet.write_dynamic_array_formula("E66:L66", "=E39:L39+E40:L40+E41:L41")
    worksheet.write_dynamic_array_formula("E67:L67", "=E40:L40+E41:L41")
    worksheet.write_dynamic_array_formula("E68:L68", "=E42:L42")
    worksheet.write_dynamic_array_formula("E69:L69", "=E43:L43")
    worksheet.write_dynamic_array_formula("E70:L70", "=E44:L44")
    worksheet.write_dynamic_array_formula("E71:L71", "=E45:L45")
    worksheet.write_dynamic_array_formula(
        "E72:L72", "=E60:L60+E62:L62+E64:L64+E66:L66+E68:L68+E69:L69+E70:L70+E71:L71"
    )  # Earnings before interest and tax (EBIT)
    worksheet.write_dynamic_array_formula(
        "E73:L73", "=IF(ISERROR(E72:L72/E50:L50),0,(E72:L72/E50:L50))"
    )  # % of sales


def add_formula_sg(worksheet):
    # Dynamic array
    worksheet.write_dynamic_array_formula("E79:L79", "=E50:L50-E51:L51-E52:L52")
    worksheet.write_dynamic_array_formula(
        "E79:L79", "(E10:L10+E12:L12+E13:L13+E16:L16+E22:L22+E30:L30+E31:L31)*-1"
    )


def main():
    # Filenames
    input_items = path / "data" / "items.csv"
    input_pl = path / "data" / "PLs.csv"
    output_file = path / "output" / "MYP_template.xlsx"
    sheet_names = ["Korea", "India", "Japan", "Thailand"]

    # Read data
    with open(input_items, "r", encoding="utf-8") as f:
        csv_reader = csv.reader(f)
        items = list(csv_reader)

    with open(input_pl, "r", encoding="utf-8") as f:
        csv_reader = csv.reader(f)
        data = list(csv_reader)

    # Year data
    fy = [2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030, ""] * 27  # no. of section

    # Zero values
    zero = [0, 0, 0, 0, 0, 0, 0, 0, ""] * 27  # no. of section

    # Create an Excel workbook
    with xlsxwriter.Workbook(output_file) as workbook:
        worksheets = [workbook.add_worksheet(name) for name in sheet_names]

        for ws in worksheets:
            add_text(ws)
            add_items(ws, items)
            add_poc(ws, data)
            add_years(ws, fy)
            add_zero(ws, zero)
            add_formula_vt(ws)
            add_formula_sg(ws)

    print("A file is created")


if __name__ == "__main__":
    main()
