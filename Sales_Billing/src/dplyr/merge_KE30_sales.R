library(dplyr)
library(readr)
library(purrr)
library(fs)
library(stringr)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Billing")


# Functions
read_txt_file <- function(file_path) {
  # Read tsv file
  df <- read_tsv(file_path,
    skip = 7,
    locale = locale(encoding = "UTF-16LE"),
    col_types = cols(
      "RecordType" = col_character(),
      "Customer" = col_character(),
      "AAG" = col_character(),
      "Dv" = col_character(),
    ),
  )
  return(df)
}


read_multiple_files <- function(list_of_files) {
  df <- list_of_files |>
    map_dfr(read_txt_file)
  return(df)
}


remove_columns <- function(df) {
  df <- df |>
    select(!c("x1", "x4"))
  return(df)
}


remove_missing_values <- function(df) {
  df <- df |>
    filter(!is.na(df$record_type))
  return(df)
}


extract_profit_center <- function(df) {
  df <- df |>
    mutate(
      profit_center = str_replace(.data$profit_center, "5668/", "")
    )
  return(df)
}


main <- function() {
  # Variables
  year <- "2024"

  # Path
  # data_path <- here(path, "data", "KE30", "Archive", glue("{year}"))
  data_path <- here(path, "data", "KE30")  # current year

  # Filenames
  input_files <- dir_ls(data_path, regexp = "\\.XLS|\\.xls")
  output_file <- here(path, "db", glue("KE30_{year}.csv"))

  # Read data
  df <- read_multiple_files((input_files)) |>
    clean_names()

  # Process data
  df <- df |>
    remove_columns() |>
    remove_missing_values() |>
    extract_profit_center()

  # Write data
  write_csv(df, output_file, na = "")
  print("A file is created")
}

main()
