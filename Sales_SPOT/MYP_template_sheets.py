import csv
import xlsxwriter


def convert_csv_to_excel(csv_file, worksheet):
    """
    Reads a CSV file with a single column and writes the data to an Excel sheet,
    handling empty lines.

    Args:
        csv_file (str): Path to the CSV file.
        worksheet (str): sheet name of an excel file
    """
    # Open the CSV file for reading
    with open(csv_file, "r", encoding="utf-8") as f:
        csv_reader = csv.reader(f)

        # Write the data from the CSV file, handling empty lines
        row = 3
        for data in csv_reader:
            # Check if the row is not empty before accessing data[0]
            if data:
                worksheet.write(row, 3, data[0])
            row += 1


def main():
    csv_file = "items.csv"
    excel_file = "MYP_template_sheets.xlsx"
    sheet_names = ["Korea", "India", "Japan", "Thailand"]

    # Create an Excel workbook
    workbook = xlsxwriter.Workbook(excel_file)

    # Create worksheets with specified names
    worksheets = [workbook.add_worksheet(name) for name in sheet_names]

    for worksheet in worksheets:
        convert_csv_to_excel(csv_file, worksheet)

    # Close the workbook
    workbook.close()

    print(f"Successfully converted {csv_file} to {excel_file} with multiple sheets")


if __name__ == "__main__":
    main()
