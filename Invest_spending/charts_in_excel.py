import pandas as pd
from pathlib import Path
from vincent.colors import brews


# Path
path = Path.cwd()

# Input file
input_file = path / "output" / "Monthly Spending FC10+2.csv"
output_file = path / "report" / "Charts.xlsx"

# Read data
df = pd.read_csv(input_file)

# Filter data
df = df[(df["category"] == "Top 15 projects") & (df["version"] != "Actual")]

# Pivot table
df = df.pivot_table(
    values="k_eur", index=["master_id", "master", "version"], columns="quarter", aggfunc="sum"
).reset_index()

# Get the dimensions of the dataframe.
(max_row, max_col) = df.shape

# Unique values
unique_categories = df["master_id"].unique()


# Create an Excel file
with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    for category in unique_categories:
        # Create a DataFrame for the current category
        category_df = df[df["master_id"] == category]

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

        # # Add the Excel table structure.
        # worksheet.add_table(
        #     0,
        #     0,
        #     max_row,
        #     max_col - 1,
        #     {
        #         "columns": column_settings,
        #         "style": "Table Style Medium 3",
        #         "name": category,
        #     },
        # )

        # Add your formatting options, for example, bold headers
        header_format = workbook.add_format({"bold": True})

        # Write the header row explicitly with your formatting
        for col_num, value in enumerate(category_df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # Create a chart object.
        chart = workbook.add_chart({"type": "column"})

        # Configure the series of the chart from the dataframe data.
        for row_num in range(1, max_row + 1):
            chart.add_series(
                # [sheetname, first_row, first_col, last_row, last_col]
                {
                    "name": [category, row_num, 3 - 1],
                    "categories": [category, 0, max_col - 4, 0, max_col - 1],
                    "values": [category, row_num, max_col - 4, row_num, max_col - 1],
                    "data_labels": {"value": True},
                    "fill": {"color": brews["Spectral"][row_num - 1]},
                    "gap": 300,
                }
            )

        # Configure the chart axes.
        chart.set_y_axis({"major_gridlines": {"visible": False}})

        # Insert the chart into the worksheet.
        worksheet.insert_chart("K2", chart)

        # Define a numeric format, such as "0" for two decimal places
        numeric_format = workbook.add_format({"num_format": "0"})

        # Apply the numeric format to the numeric column (adjust the range as needed)
        worksheet.set_column("C:F", None, numeric_format)
