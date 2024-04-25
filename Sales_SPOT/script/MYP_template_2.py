import csv
import xlsxwriter
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def add_items(list_of_data, worksheet):
    # Start from the cell. Rows and columns are zero indexed.
    row = 3
    col = 3

    # Write the data from the CSV file, handling empty lines
    for data in list_of_data:
        # Check if the row is not empty before accessing data[0]
        if data:
            worksheet.write(row, col, data[0])
        row += 1


def add_div(list_of_data, worksheet):
    # Start from the cell. Rows and columns are zero indexed.
    row = 1
    col = 4

    for data in list_of_data:
        worksheet.write(row, col, data[0])  # Division
        col += 9


def add_bu(list_of_data, worksheet):
    # Start from the cell. Rows and columns are zero indexed.
    row = 2
    col = 4

    for data in list_of_data:
        worksheet.write(row, col, data[1])  # BU
        col += 9


def add_pl(list_of_data, worksheet):
    # Start from the cell. Rows and columns are zero indexed.
    row = 3
    col = 4

    for data in list_of_data:
        worksheet.write(row, col, data[2])  # Productlines
        col += 9


def add_years(list_of_data, worksheet):
    # Start from the cell. Rows and columns are zero indexed.
    row = 5
    col = 4

    for data in list_of_data:
        worksheet.write(row, col, data)
        col += 1


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

    # Create an Excel workbook
    with xlsxwriter.Workbook(output_file) as workbook:
        worksheets = [workbook.add_worksheet(name) for name in sheet_names]

        for worksheet in worksheets:
            add_items(items, worksheet)
            add_div(data, worksheet)
            add_bu(data, worksheet)
            add_pl(data, worksheet)
            add_years(fy, worksheet)

    print("A file is created")


if __name__ == "__main__":
    main()
