import csv
import xlsxwriter


def convert_csv_to_excel(csv_file, excel_file):
    """
    Reads a CSV file with a single column and writes the data to an Excel file,
    handling empty lines.

    Args:
        csv_file (str): Path to the CSV file.
        excel_file (str): Path to the output Excel file.
    """
    # Open the CSV file for reading
    # Create an Excel workbook and worksheet
    workbook = xlsxwriter.Workbook(excel_file)
    worksheet = workbook.add_worksheet()

    with open(csv_file, "r", encoding="utf-8") as f:
        csv_reader = csv.reader(f)

        # Write the data from the CSV file, handling empty lines
        row = 3
        for data in csv_reader:
            # Check if the row is not empty before accessing data[0]
            if data:
                worksheet.write(row, 3, data[0])
            row += 1

    # Close the workbook
    workbook.close()


def main():

    csv_file = "items.csv"
    excel_file = "MYP_template.xlsx"
    convert_csv_to_excel(csv_file, excel_file)

    print(f"Successfully converted {csv_file} to {excel_file}")


if __name__ == "__main__":
    main()
