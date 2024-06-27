import pandas as pd
from pathlib import Path
from excel_formatting import (
    add_excel_table,
    apply_header_formatting,
    apply_other_formatting,
)


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

            # Add header formatting
            apply_header_formatting(category_df, workbook, worksheet)

            # Add various other formatting
            apply_other_formatting(workbook, worksheet)

    print("A file is created")


if __name__ == "__main__":
    main()
