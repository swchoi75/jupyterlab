import csv
import xlsxwriter


def convert_csv_to_excel(csv_file, excel_file, sheet_names):
    """
    Reads a CSV file with a single column and writes the data to the different sheets
    in an Excel file, handling empty lines and specifying encoding.

    Args:
        csv_file (str): Path to the CSV file.
        excel_file (str): Path to the output Excel file.
        sheet_names (list): List of names for the sheets in the Excel file.
    """
    # Create an Excel workbook
    workbook = xlsxwriter.Workbook(excel_file)

    # Create worksheets with specified names
    worksheets = [workbook.add_worksheet(name) for name in sheet_names]
    for worksheet in worksheets:
        # Open the CSV file for reading with specified encoding
        with open(csv_file, "r", encoding="utf-8") as f:
            csv_reader = csv.reader(f)

            # Write the data from the CSV file to separate sheets, handling empty lines
            row = 3
            for data in csv_reader:
                # Check if the row is not empty before accessing data[0]
                if data:
                    worksheet.write(row, 3, data[0])
                row += 1

    # Close the workbook
    workbook.close()


# Example usage
csv_file = "items.csv"
excel_file = "MYP_template_sheets.xlsx"
sheet_names = ["Korea", "India", "Japan", "Thailand"]
convert_csv_to_excel(csv_file, excel_file, sheet_names)

print(f"Successfully converted {csv_file} to {excel_file} with multiple sheets")
