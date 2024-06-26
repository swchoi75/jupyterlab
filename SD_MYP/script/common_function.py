import pandas as pd
from janitor import clean_names


# Functions
def clean_column_names(df):
    """Apply the cleaning function to all column names"""

    # clean new lines
    df.columns = df.columns.map(lambda x: x.replace("\n", ""))

    df = df.clean_names()

    # clean trailing underscore
    df.columns = df.columns.map(lambda x: x.lstrip("_").rstrip("_"))

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
