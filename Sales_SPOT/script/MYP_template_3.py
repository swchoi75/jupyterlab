import openpyxl as op
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions


def main():
    # Filenames
    input_file = path / "output" / "MYP_template_op.xlsx"
    output_file = path / "output" / "MYP_template_op2.xlsx"
    sheet_names = ["Korea", "India", "Japan", "Thailand"]

    # Read from an Excel workbook
    wb = op.load_workbook(input_file)

    # Copy & Rename worksheet
    ws = wb["Sheet"]
    for name in sheet_names:
        ws_copy = wb.copy_worksheet(ws)
        ws_copy.title = name

    # Remove worksheet
    wb.remove(ws)

    # Print worksheet names
    print(wb.sheetnames)

    # Save workbook
    wb.save(output_file)

    print("A file is upated")


if __name__ == "__main__":
    main()
