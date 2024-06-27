import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def read_data(input_file, meta_file):
    # Read data
    list = pd.read_csv(input_file)
    members = pd.read_csv(meta_file).rename(columns={"first_name": "responsible"})

    # Vlookup Responsible names
    df = pd.merge(list, members, on="email")
    return df


def fileter_data(df, list_of_years):
    # Filter active members, open action items in 2021 - 2024
    df = df[
        (df["active"] == "yes")
        & (df["status"].isin(["Action", "Decision"]))
        & (df["year"].isin(list_of_years))
    ]
    return df


def sort_data(df):
    return df.sort_values(by=["year", "calendarweek"], ascending=False)


def rename_columns(df):
    df = df.rename(
        columns={
            "year": "Year",
            "calendarweek": "CW",
            "title": "Title",
            "description": "Description",
            # "function": "Function",
            "status": "Status",
            "duedate": "Due_date",
        }
    )
    return df


def select_columns(df):
    return df[
        ["Year", "CW", "Title", "Description", "Status", "Due_date", "responsible"]
    ]


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


def apply_header_format(df, workbook, worksheet):
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


def apply_formatting(workbook, worksheet):
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

    # Enable text wrapping for an entire column
    column_format = workbook.add_format()
    column_format.set_text_wrap()  # seems to be NOT working

    # Freeze panes
    worksheet.freeze_panes(1, 0)

    # Set zoom
    worksheet.set_zoom(100)

    # You can apply additional formatting to cells as needed


def main():

    # Variables
    list_of_years = [2021, 2022, 2023, 2024]

    # Input file & Output file
    input_file = path / "output" / "output_jour_fixe.csv"
    meta_file = path / "meta" / "email.csv"
    output_file = path / "output" / "report_jour_fixe.xlsx"

    # Read data
    df = read_data(input_file, meta_file)

    # Process data
    df = (
        df.pipe(fileter_data, list_of_years)
        .pipe(sort_data)
        .pipe(rename_columns)
        .pipe(select_columns)
    )

    # Create a new Excel file and write separate sheets for each category
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:

        # Unique values
        unique_categories = df["responsible"].unique()

        for category in unique_categories:
            # Create a DataFrame for the current category
            category_df = df[df["responsible"] == category]
            category_df = category_df.drop(columns=["responsible"])

            # Write the dataframe data to XlsxWriter. Turn off the default header and
            # index and skip one row to allow us to insert a user defined header.
            category_df.to_excel(
                writer, sheet_name=category, startrow=1, header=False, index=False
            )

            # Access the xlsxwriter workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets[category]

            # Add Excel table
            add_excel_table(category_df, worksheet, category)

            # Add header format
            apply_header_format(category_df, workbook, worksheet)

            # Add formatting
            apply_formatting(workbook, worksheet)

    print("A file is created")


if __name__ == "__main__":
    main()
