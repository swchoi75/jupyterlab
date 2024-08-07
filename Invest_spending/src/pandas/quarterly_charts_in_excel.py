import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent.parent.parent


# Functions
def add_column(df):
    df["m_eur"] = df["k_eur"] / 1000
    return df


def rename_column(df):
    df = df.rename(
        columns={
            "division_receiver": "division",
            "bu_receiver": "bu",
            "outlet_receiver": "outlet",
        }
    )
    return df


def filter_data(df):
    df = df[(df["category"] == "Top 15 projects") & (df["version"] != "Actual")]

    pl = df[(df["assignment"] == "DIV E") | (df["assignment"] == "DIV P")]
    cf = df[df["assignment"] == "Central function"]
    npf = df[df["assignment"] == "NPF"]

    df = pd.concat([pl, cf, npf])
    return df


def select_columns(df, list_of_columns):
    df = df[
        list_of_columns
        + [
            "quarter",
            "version",
            "m_eur",
        ]
    ]
    return df


def sort_by_values(df):
    """Sort by values, but this sort does not show up in the excel report."""
    df = df.sort_values(
        ["assignment", "bu", "outlet", "version", "quarter"],
        ascending=[True, True, True, True, False],
    )
    return df


def pivot_wider_by_version(df, list_of_columns):
    df = (
        df.pivot_table(
            values="m_eur",
            index=list_of_columns,
            columns="version",
            aggfunc="sum",
        )
        .fillna(0)  # Fill NA with zero for quarter: Q1, Q2, Q3, Q4
        .reset_index()
    )
    return df


def pivot_longer(df, list_of_columns):
    df = df.melt(
        id_vars=list_of_columns,
        value_vars=["Budget", "FC"],
        value_name="m_eur",
    )
    return df


def pivot_wider_by_quarter(df, list_of_columns):
    df = (
        df.pivot_table(
            values="m_eur",
            index=list_of_columns,
            columns="quarter",
            aggfunc="sum",
        )
        .fillna(0)  # Fill NA with zero for quarter: Q1, Q2, Q3, Q4
        .reset_index()
    )
    return df


def main():
    # Filenames
    input_file = path / "output" / "Monthly Spending FC10+2.csv"
    meta_file = path / "data" / "top 15 projects.csv"
    output_file = path / "report in Excel" / "Quarterly Charts.xlsx"

    # Read data
    df_1 = pd.read_csv(input_file)
    df_2 = pd.read_csv(meta_file, usecols=[0, 2])
    df = pd.merge(df_1, df_2, on="master_id")

    # Process data
    key_columns = [
        "master_id",
        "master",
        # "division",
        "assignment",
        "bu",
        "outlet",
        "location",
    ]

    df = (
        df.pipe(add_column)
        .pipe(rename_column)
        .pipe(filter_data)
        .pipe(select_columns, key_columns)
        .pipe(sort_by_values)
        .pipe(pivot_wider_by_version, key_columns + ["quarter"])
        .pipe(pivot_longer, key_columns + ["quarter"])
        .pipe(pivot_wider_by_quarter, key_columns + ["version"])
    )

    # # Get the dimensions of the dataframe.
    # (max_row, max_col) = df.shape

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
                        "name": [category, row_num, 6],
                        "categories": [category, 0, max_col - 4, 0, max_col - 1],
                        "values": [
                            category,
                            row_num,
                            max_col - 4,
                            row_num,
                            max_col - 1,
                        ],
                        "data_labels": {"value": True},
                        "gap": 300,
                    }
                )

            # Configure the chart axes.
            chart.set_y_axis({"major_gridlines": {"visible": False}})

            # Insert the chart into the worksheet.
            worksheet.insert_chart("B10", chart)

            # Define a numeric format, such as "0.0" for two decimal places
            numeric_format = workbook.add_format({"num_format": "0.0"})

            # Apply the numeric format to the numeric column (adjust the range as needed)
            worksheet.set_column("A:Z", None, numeric_format)

            # Specify column widths
            worksheet.set_column("B:B", 30)  # Title
            worksheet.set_column("C:C", 10)  # Title

    print("A file is created")


if __name__ == "__main__":
    main()
