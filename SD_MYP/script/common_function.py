import pandas as pd
from janitor import clean_names


# Functions
def clean_new_lines(column_name):
    """Functions to clean column names"""
    return column_name.replace("\n", "")


def clean_trailing_underscore(column_name):
    """Functions to clean column names"""
    return column_name.lstrip("_").rstrip("_")


def clean_column_names(df):
    """Apply the cleaning function to all column names"""
    df.columns = df.columns.map(clean_new_lines)
    df = df.clean_names()
    df.columns = df.columns.map(clean_trailing_underscore)
    return df


def export_columns_to_csv(df, filename):
    """
    This function exports the column names of a DataFrame to a CSV file,
    with each column name in a separate row.
    """

    # Get column names as a Series
    column_names = df.columns

    # Create a DataFrame with a single column named "Column Name"
    df_columns = pd.DataFrame(column_names, columns=["Column Name"])

    # Save the DataFrame to a CSV file
    df_columns.to_csv(filename, index=False)

    print("Column names exported")
