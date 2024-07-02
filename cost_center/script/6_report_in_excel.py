import pandas as pd
from pathlib import Path
from common_function import add_headcount_sheet, add_summary_sheet, add_cc_sheet


# Path
path = Path(__file__).parent.parent


# Functions
def filter_by_responsible(df, responsible):
    df = df[df["responsible"] == responsible]
    return df


def remove_columns(df, cols_to_remove):
    df = df[[col for col in df.columns if col not in cols_to_remove]]
    return df


def remove_subtotal_rows(df, col_name):
    df = df[~df[col_name].str.contains(" - subtotal")]
    return df


def main():

    # Variables
    from common_variable import year, month, responsible_name

    skiprows = 6

    # Filenames
    input_hc = path / "output" / "4-1_headcount_report.csv"
    input_summary = path / "output" / "3-1_summary_cost_report.csv"
    input_file = path / "output" / "3-3_further_refine_cost_report.csv"
    output_file = (
        # :0>: This pads the number with zeros from the left side.
        path
        / "report"
        / f"{year}-{month:0>2}_CC report for_{responsible_name}.xlsx"
    )

    # Read data
    df_hc = pd.read_csv(input_hc, dtype={"cctr": str})
    df_summary = pd.read_csv(input_summary, dtype={"cctr": str})
    df = pd.read_csv(input_file, dtype={"cctr": str})

    # Process data
    df_hc = df_hc.pipe(filter_by_responsible, responsible_name).pipe(
        remove_columns, "responsible"
    )

    df_summary = df_summary.pipe(filter_by_responsible, responsible_name).pipe(
        remove_columns, "responsible"
    )

    df = (
        df.pipe(filter_by_responsible, responsible_name)
        .pipe(remove_columns, "responsible")
        .pipe(remove_subtotal_rows, "cctr")
    )

    # Write data
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:

        # Headcount sheet
        df_hc.to_excel(
            writer,
            sheet_name="headcount",
            startrow=1 + skiprows,
            header=False,
            index=False,
        )

        add_headcount_sheet(
            writer, df_hc, "headcount", year, month, responsible_name, skiprows
        )

        # Summary sheet
        df_summary.to_excel(
            writer,
            sheet_name="summary",
            startrow=1 + skiprows,
            header=False,
            index=False,
        )

        add_summary_sheet(
            writer, df_summary, "summary", year, month, responsible_name, skiprows
        )

        # Cost center sheet
        unique_categories = df["cctr"].unique()

        for category in unique_categories:
            # Create a DataFrame for the current category
            category_df = df[df["cctr"] == category]

            # Write the dataframe data to XlsxWriter. Turn off the default header and
            # index and skip one row to allow us to insert a user defined header.
            category_df.to_excel(
                writer,
                sheet_name=category,
                startrow=1 + skiprows,
                header=False,
                index=False,
            )

            add_cc_sheet(writer, df, category, year, month, responsible_name, skiprows)

    print("A file is created")


if __name__ == "__main__":
    main()
