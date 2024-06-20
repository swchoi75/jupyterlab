library(dplyr)
library(readr)
library(purrr)
library(stringr)
library(readxl)
library(fs)
library(janitor)
library(here)


# Path
path <- here("PnL")


# Functions
read_multiple_files <- function(list_of_files) {
  df <- list_of_files |>
    map_dfr(read_excel,
      sheet = "Total devaluation",
      range = "B4:AM30000",
      .id = "source"
    )
  return(df)
}

remove_na_rows <- function(df) {
  df <- df |>
    filter(!is.na(.data[["plant"]]))
  return(df)
}

year_month <- function(df) {
  # Capture Year and Month
  df <- df |>
    mutate(
      source = str_extract(source, "\\d{4}-\\d{2}"),
    )
  return(df)
}

filter_data <- function(df) {
  df <- df |>
    filter(.data[["monthly_impact"]] > 10^7 | .data[["monthly_impact"]] < -10^7)
  return(df)
}


main <- function() {
  # Path
  data_path <- here(path, "data", "Inventory devaluation")

  # Filenames
  xls_files <- dir_ls(data_path, regexp = "\\.xlsx$")
  output_file <- here(path, "output", "Inventory devaluation.csv")

  # Read data
  df <- read_multiple_files((xls_files))

  # Process data
  df <- df |>
    clean_names() |>
    remove_na_rows() |>
    year_month() |>
    filter_data()

  # Write data
  write_csv(df, output_file)
  print("A file is created")
}

main()
