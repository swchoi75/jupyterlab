import xlsxwriter
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def write_data(worksheet):
    # Some data we want to write to the worksheet.
    expenses = (
        ["Rent", 1000],
        ["Gas", 100],
        ["Food", 300],
        ["Gym", 50],
    )

    # Start from the first cell. Rows and columns are zero indexed.
    row = 0
    col = 0

    # Iterate over the data and write it out row by row.
    for item, cost in expenses:
        worksheet.write(row, col, item)
        worksheet.write(row, col + 1, cost)
        row += 1

    # Write a total using a formula.
    worksheet.write(row, 0, "Total")
    worksheet.write(row, 1, "=SUM(B1:B4)")


def main():
    output_file = path / "output" / "Expenses01.xlsx"

    # Create a workbook and add a worksheet.
    with xlsxwriter.Workbook(output_file) as workbook:
        worksheet = workbook.add_worksheet()

        write_data(worksheet)

    print("A file is created")


if __name__ == "__main__":
    main()
