library(dplyr)
library(readr)
library(purrr)
library(fs)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Billing")


# Functions
read_txt_file <- function(file_path) {
  # Read tsv file
  df <- read_tsv(file_path,
    skip = 5,
    locale = locale(encoding = "UTF-16LE"),
    col_types = cols(
      "Reference" = col_character(),
      "Customer" = col_character(),
    ),
    show_col_types = FALSE,
  )
  # Correct the occasional column name in the data source
  if ("Amount in DC" %in% names(df)) {
    df <- df |>
      rename(c(
        "Amount in doc. curr." = "Amount in DC",
        "Amount in local cur." = "Amt in loc.cur."
      ))
  }
  return(df)
}


read_multiple_files <- function(list_of_files) {
  df <- list_of_files |>
    map_dfr(read_txt_file)
  return(df)
}


remove_columns <- function(df) {
  df <- df |>
    select(!c("x1", "x2", "x6"))
  return(df)
}


remove_missing_values <- function(df) {
  df <- df |>
    filter(!is.na(df$g_l))
  return(df)
}


main <- function() {
  # Variables
  year <- "2024"

  # Path
  # data_path <- here(path, "data", "GL", "Archive", glue("{year}"))
  data_path <- here(path, "data", "GL") # for current year

  # Filenames
  input_files <- dir_ls(data_path, regexp = "\\.XLS|\\.xls")
  output_file <- here(path, "db", glue("GL_{year}.csv"))

  # Read data
  df <- read_multiple_files((input_files)) |>
    clean_names()

  # Process data
  df <- df |>
    remove_columns() |>
    remove_missing_values()

  # Write data
  write_csv(df, output_file, na = "")
  print("A file is created")
}

main()
