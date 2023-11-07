import pandas as pd
from pathlib import Path
# import xlsxwriter  # pip install XlsxWriter


# Path
path = Path(r"C:\Users\uid98421\Vitesco Technologies\Controlling VT Korea - Documents\120. Data automation\1000 VT Datalake")

# Input file
input_file = path / "output/Investment_spending/" / "Monthly Spending FC10+2.csv"
output_file = path / "report in Excel/Investment_spending" / "Charts.xlsx"


df = pd.read_csv(input_file)

df = df[(df["category"] == "Top 15 projects") & (df["version"] != "Actual")]

df = df.groupby(["version", "master", "master_id", "quarter",
                 "division_receiver", "bu_receiver", "outlet_receiver", "location"]
                ).agg(spending=("k_eur", "sum")).reset_index()

df = df.sort_values(["version", "quarter", "spending"],
                    ascending=[True, True, False])


# Unique values
unique_categories = df["master_id"].unique()

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
        column_settings = [{"header": column}
                           for column in category_df.columns]

        # Write the header row explicitly with your formatting
        for col_num, value in enumerate(category_df.columns.values):
            worksheet.write(0, col_num, value)
