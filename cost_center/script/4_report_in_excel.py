import pandas as pd
from pathlib import Path
from excel_formatting import add_summary_sheet, add_cc_sheet


# Path
path = Path(__file__).parent.parent


# Functions
def main():

    # Variables
    from common_variable import year, month, responsible_name

    skiprows = 6

    # Filenames
    input_hc = path / "output" / "0_headcount_report.csv"
    input_summary = path / "output" / "3-0_fix_act_to_plan_summary.csv"
    input_file = path / "output" / "3-2_further_refine_report.csv"
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

    # Write data
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:

        # Summary sheet
        df_summary.to_excel(
            writer,
            sheet_name="summary",
            startrow=1 + skiprows,
            header=False,
            index=False,
        )

        add_summary_sheet(writer, df, year, month, responsible_name, skiprows)

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
