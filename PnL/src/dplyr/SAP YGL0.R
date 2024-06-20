library(dplyr)
library(readr)
library(purrr)
library(stringr)
library(fs)
library(here)


# Path
path <- here("PnL")


# Functions
read_multiple_files <- function(list_of_files) {
  df <- list_of_files |>
    map_dfr(read_tsv,
      col_types = cols(.default = col_character()),
      .id = "source"
    ) |>
    select(c("source", "OneGL B/S + P/L", "01":"12"))
  return(df)
}

process_textual_columns <- function(df) {
  # Extract RACE account, G/L accounts, Profit center in number format
  df <- df |>
    mutate(
      source = str_extract(.data[["source"]], "[0-9\\-]{8,9}"),
      Key = str_extract(.data[["OneGL B/S + P/L"]], "[0-9]+|^K[0-9]+|^P[0-9]+")
    )
  return(df)
}

process_numeric_columns <- function(df) {
  # Remove thousand separators "," and Convert types
  month <- c(
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"
  )
  df <- df |>
    mutate(
      across(all_of(month), ~ str_remove_all(.x, ",")),
      across(all_of(month), as.double),
      across(all_of(month), ~ tidyr::replace_na(.x, 0))
    )
  return(df)
}

lookup_table <- function(filename) {
  df <- read_csv(filename,
    col_types = cols(.default = col_character()),
  ) |>
    # Extract RACE account, G/L account in number format ----
    mutate(Key = str_extract(.data[["Key"]], "[0-9]+|^K[0-9]+|^P[0-9]+"))
  return(df)
}

join_with_lookup <- function(df, filename) {
  df <- df |>
    left_join(lookup_table(filename), by = "Key") |>
    filter(.data[["A"]] != "")
  return(df)
}

change_sign_logic <- function(df) {
  # Sign logic change from SAP to RACE
  month <- c(
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"
  )
  df <- df |>
    mutate(across(all_of(month), ~ .x / -10^3))
  return(df)
}

reorder_columns <- function(df) {
  df <- df |>
    select(c(
      "source", "A", "B", "C", "D",
      "01":"12",
      "Key", "OneGL B/S + P/L"
    ))
  return(df)
}

change_column_names <- function(df) {
  df <- df |>
    rename(c(
      PrCr = "source",
      Jan = "01", Feb = "02", Mar = "03", Apr = "04", May = "05", Jun = "06",
      Jul = "07", Aug = "08", Sep = "09", Oct = "10", Nov = "11", Dec = "12"
    ))
  return(df)
}

main <- function() {
  # Path
  data_path <- here(path, "data", "SAP YGL0")

  # Filenames
  txt_files <- dir_ls(data_path, regexp = "\\.dat$")
  meta_file <- here(path, "meta", "Lookup_table.csv")
  output_file <- here(path, "output", "SAP YGL0 P&L.csv")

  # Read data
  df <- read_multiple_files(txt_files)

  # Process data
  df <- df |>
    process_textual_columns() |>
    process_numeric_columns() |>
    join_with_lookup(meta_file) |>
    change_sign_logic() |>
    reorder_columns() |>
    change_column_names()

  # Write data
  write_csv(df, output_file)
  print("A file is created")
}

main()
