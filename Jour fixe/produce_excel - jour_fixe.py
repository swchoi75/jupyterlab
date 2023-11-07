import pandas as pd
from pathlib import Path
# import xlsxwriter  # pip install XlsxWriter


# Path
# path = Path('./datasets/home-dataset/data/')
path = Path(
    r"C:\Users\uid98421\OneDrive - Vitesco Technologies\FY 2023\Jour fixe")


# Input file & Output file
input_file = path / "output_jour_fixe.csv"
meta_file = path / "email.csv"
output_file = path / "report_jour_fixe.xlsx"


# Read data
list = pd.read_csv(input_file)
members = pd.read_csv(meta_file).rename(columns={"first_name": "responsible"})


# Vlookup Responsible names
df = pd.merge(list, members, on="email")


# Filter active members, open action items in 2023
df = df[
    (df["active"] == "yes") & (df["info_action"]
                               == "Action") & (df["year"] == 2023)
]


# Unique values
unique_categories = df["responsible"].unique()


# Rename columns
df = df.rename(
    columns={
        "year": "Year",
        "calendarweek": "CW",
        "title": "Title",
        "description": "Description",
        "function": "Function",
        "info_action": "Status",
    }
)

# Select columns
df = df[["Year", "CW", "Title", "Description", "Status", "responsible"]]


# Create a new Excel file and write separate sheets for each category
with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
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

        # Get the dimensions of the dataframe.
        (max_row, max_col) = category_df.shape

        # Create a list of column headers, to use in add_table().
        column_settings = [{"header": column}
                           for column in category_df.columns]

        # Add the Excel table structure. Pandas will add the data.
        worksheet.add_table(
            0,
            0,
            max_row,
            max_col - 1,
            {
                "columns": column_settings,
                "style": "Table Style Medium 3",
                "name": category,
            },
        )

        # Add your formatting options, for example, bold headers
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
        for col_num, value in enumerate(category_df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # Specify row heights
        worksheet.set_row(0, 20)

        # Specify column widths
        worksheet.set_column("C:C", 50)  # Title
        worksheet.set_column("D:D", 100)  # Description

        # Enable text wrapping for an entire column
        column_format = workbook.add_format()
        column_format.set_text_wrap()
        worksheet.set_column("C:C", 50, column_format)  # Title

        # Freeze panes
        worksheet.freeze_panes(1, 0)

        # Set zoom
        worksheet.set_zoom(100)

        # You can apply additional formatting to cells as needed
