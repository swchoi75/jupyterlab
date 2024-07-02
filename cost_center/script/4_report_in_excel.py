import pandas as pd
from pathlib import Path
from excel_formatting import (
    add_label,
    add_excel_table,
    apply_header_formatting,
    apply_conditional_formatting,
    delta_conditional_formatting,
    grand_total_conditional_formatting,
    apply_other_formatting,
)


# Path
path = Path(__file__).parent.parent


# Functions
def main():

    # Variables
    from common_variable import year, month, responsible_name

    skiprows = 6

    # Filenames
    input_file = path / "output" / "3-2_further_refine_report.csv"
    input_summary = path / "output" / "3-3_fix_act_to_plan_summary.csv"
    output_file = (
        # :0>: This pads the number with zeros from the left side.
        path
        / "report"
        / f"{year}-{month:0>2}_CC report for_{responsible_name}.xlsx"
    )

    # Read data
    df = pd.read_csv(input_file, dtype={"cctr": str})
    df_summary = pd.read_csv(input_summary, dtype={"cctr": str})

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

        # Access the xlsxwriter workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets["summary"]

        # Add label
        add_label(workbook, worksheet, year, month, responsible_name)

        # Add Excel table
        add_excel_table(df_summary, worksheet, "summary", skiprows)

        # Add header formatting
        apply_header_formatting(df_summary, workbook, worksheet, skiprows)

        # Add conditional formatting
        delta_conditional_formatting(workbook, worksheet)
        grand_total_conditional_formatting(workbook, worksheet)

        # Add various other formatting
        apply_other_formatting(workbook, worksheet, skiprows)

        # Add worksheet tab color
        worksheet.set_tab_color("yellow")

        # Unique values
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

            # Access the xlsxwriter workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets[category]

            # Add label
            add_label(workbook, worksheet, year, month, responsible_name)

            # Add Excel table
            add_excel_table(category_df, worksheet, category, skiprows)

            # Add header formatting
            apply_header_formatting(category_df, workbook, worksheet, skiprows)

            # Add conditional formatting
            apply_conditional_formatting(workbook, worksheet)
            delta_conditional_formatting(workbook, worksheet)

            # Add various other formatting
            apply_other_formatting(workbook, worksheet, skiprows)

    print("A file is created")


if __name__ == "__main__":
    main()
