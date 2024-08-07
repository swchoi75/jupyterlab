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
    )
  return(df)
}

select_columns <- function(df) {
  df <- df |> select(c(
    "source", "OneGL B/S + P/L", "01":"12"
  ))
  return(df)
}

process_textual_columns <- function(df, path_name) {
  df <- df |>
    mutate(
      source = str_remove(.data$source, path_name),
      source = str_remove(.data$source, ".dat$"),
      PrCr = str_extract(df[["OneGL B/S + P/L"]], "[0-9\\-]{8,9}")
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

change_sign_logic <- function(df) {
  # Sign logic change from SAP to RACE
  month <- c(
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"
  )
  df <- df |>
    mutate(across(all_of(month), ~ .x / -10^6))
  return(df)
}

change_column_names <- function(df) {
  lookup <- c(
    "Items" = "source",
    "Jan" = "01",
    "Feb" = "02",
    "Mar" = "03",
    "Apr" = "04",
    "May" = "05",
    "Jun" = "06",
    "Jul" = "07",
    "Aug" = "08",
    "Sep" = "09",
    "Oct" = "10",
    "Nov" = "11",
    "Dec" = "12"
  )
  df <- df |>
    rename(all_of(lookup))
  return(df)
}

main <- function() {
  # Variables
  path_name <- paste0(path, "/data/SAP YGL4/")

  # Path
  data_path <- here(path, "data", "SAP YGL4")

  # Filenames
  txt_files <- dir_ls(data_path, regexp = "\\.dat$")
  output_file <- here(path, "output", "SAP YGL4 P&L.csv")

  # Read data
  df <- read_multiple_files(txt_files)

  # Process data
  df <- df |>
    select_columns() |>
    process_textual_columns(path_name) |>
    process_numeric_columns() |>
    change_sign_logic() |>
    change_column_names() |>
    tidyr::drop_na(c("PrCr"))

  # Write data
  write_csv(df, output_file)
  print("A file is created")
}

main()
