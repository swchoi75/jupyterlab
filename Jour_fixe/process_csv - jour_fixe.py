import ast
import pandas as pd
from pandas import json_normalize
from pathlib import Path
from janitor import clean_names  # pip install pyjanitor


# Path
# path = Path('./datasets/home-dataset/data/')
path = Path.cwd()


# Input file & Output file
input_file = path / "data" / "download_jour_fixe.csv"
output_file = path / "output" / "output_jour_fixe.csv"


# Functions
def load_and_clean_data(file_path):
    df = pd.read_csv(file_path).clean_names()
    return df


def select_columns(df, selected_columns):
    df = df[selected_columns]
    return df


def apply_literal_eval(df, columns_to_eval):
    # Remove the outer double quotes to change data type from string to list
    for column in columns_to_eval:
        df[column] = df[column].apply(ast.literal_eval)
    return df


def count_dict_elements(df, column_name, new_column_name):
    # Count the elements of the list
    df[new_column_name] = df[column_name].apply(len)
    return df


def explode_column(df, column_name):
    # Explode or unnest the list elements into each rows
    df = df.explode(column_name)
    return df


def normalize_and_create_column(df, column_name, new_column_name):
    # Parse JSON and create columns with "Value" field
    normalized = json_normalize(df[column_name])
    df[new_column_name] = normalized["Value"]
    return df


def replace_substring(df, column_name, substring, replacement):
    df[column_name] = df[column_name].str.replace(substring, replacement)
    return df


def rename_column(df, old_column_name, new_column_name):
    df = df.rename(columns={old_column_name: new_column_name})
    return df


def main(input_file):
    df = (
        load_and_clean_data(input_file)
        .pipe(
            select_columns,
            selected_columns=[
                "id",
                "year",
                "calendarweek",
                "title",
                "description",
                "originator",
                "information_x002f_action_x002f_d",
                "responsible_x002f_report#claims",
            ],
        )
        .pipe(
            apply_literal_eval,
            columns_to_eval=[
                "originator",
                "information_x002f_action_x002f_d",
                "responsible_x002f_report#claims",
            ],
        )
        .pipe(
            count_dict_elements,
            column_name="responsible_x002f_report#claims",
            new_column_name="DictCount",
        )
        .pipe(explode_column, column_name="responsible_x002f_report#claims")
    )

    # Reset the index after exploding
    df = df.reset_index(drop=True)

    df = (
        df.pipe(
            normalize_and_create_column,
            column_name="originator",
            new_column_name="function",
        )
        .pipe(
            normalize_and_create_column,
            column_name="information_x002f_action_x002f_d",
            new_column_name="info_action",
        )
        .pipe(
            replace_substring,
            column_name="responsible_x002f_report#claims",
            substring="i:0#.f|membership|",
            replacement="",
        )
        .pipe(
            rename_column,
            old_column_name="responsible_x002f_report#claims",
            new_column_name="email",
        )
        # Select the final set of columns
        .pipe(
            select_columns,
            selected_columns=[
                "id",
                "year",
                "calendarweek",
                "title",
                "description",
                # after parsing JSON
                "function",
                "info_action",
                "email",
            ],
        )
    )

    return df


resulting_df = main(input_file)


# Output data
resulting_df.to_csv(output_file, index=False)
