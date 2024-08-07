import pandas as pd
from pathlib import Path


# Path
absolute_path = (
    r"C:\Users\uid98421\OneDrive - Vitesco Technologies\GitHub\jupyterlab\Jour_fixe"
)
path = Path(absolute_path)


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

    df = (
        read_data(input_file, meta_file)
        .pipe(fileter_data, list_of_years)
        .pipe(sort_data)
    )

    # Unique values
    unique_categories = df["responsible"].unique()

    df = df.pipe(rename_columns).pipe(select_columns)

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
            column_settings = [{"header": column} for column in category_df.columns]

            # Add the Excel table structure.
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
            worksheet.set_column("E:F", 11)  # Status, Due_date

            # Enable text wrapping for an entire column
            column_format = workbook.add_format()
            column_format.set_text_wrap()
            worksheet.set_column("C:C", 50, column_format)  # Title

            # Freeze panes
            worksheet.freeze_panes(1, 0)

            # Set zoom
            worksheet.set_zoom(100)

            # You can apply additional formatting to cells as needed


if __name__ == "__main__":
    main()


########## Sharepoint connection ##########
import sys

sys.path.append(absolute_path)
from SharepointFunctions import SharepointFunctions as SF


# User accounts
username = "uid98421@vitesco.com"  # <-- input username
f = open(path / "meta" / "account.txt")
lines = f.readlines()
password = lines[0]


# connect to Business OneDrive
sharepoint_url = "https://vitesco.sharepoint.com/teams/team_10016532/"
sp = SF(username, password, sharepoint_url)


# list files in sharepoint folder
folder_url = "/teams/team_10016532/Shared Documents/120. Data automation/jupyterlab/Jour_fixe/output/"

file_list = sp.get_filelist_sharepoint(folder_url)
# for file in file_list:
#     print(file.properties["Name"])

# Output files
output_file = path / "output" / "report_jour_fixe.xlsx"
output_2 = path / "data" / "download_jour_fixe.xlsx"

# read file from local
file_data = sp.read_file_local(output_file)
file_2 = sp.read_file_local(output_2)


# write file to sharepoint
file_url = "/teams/team_10016532/Shared Documents/120. Data automation/jupyterlab/Jour_fixe/output/report_jour_fixe.xlsx"
file_url_2 = "/teams/team_10016532/Shared Documents/120. Data automation/jupyterlab/Jour_fixe/data/download_jour_fixe.xlsx"
sp.write_file_sharepoint(file_data, file_url)
sp.write_file_sharepoint(file_2, file_url_2)

########## Sharepoint connection ##########
