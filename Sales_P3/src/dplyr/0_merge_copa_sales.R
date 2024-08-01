library(dplyr)
library(readr)
library(arrow)
library(purrr)
library(fs)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_P3")


# Functions
read_txt_file <- function(file_path) {
  # Read tsv file
  df <- read_tsv(
    file_path,
    skip = 10,
    locale = locale(encoding = "UTF-16LE"),
    col_types = cols(
      "Period" = col_character(),
      "CoCd" = col_character(),
      "Doc. no." = col_character(),
      "Ref.doc.no" = col_character(),
      "AC DocumentNo" = col_character(),
      "Delivery" = col_character(),
      # "Item" = col_integer(),
      "Plnt" = col_character(),
      "Tr.Prt" = col_character(),
      "ConsUnit" = col_character(),
      "FIRE Plant" = col_character(),
      "FIREOutlet" = col_character(),
    )
  )
  # Correct the occasional column name in the data source
  if ("Stock val." %in% names(df)) {
    df <- df |>
      rename(c("Stock Value" = "Stock val."))
  }
  return(df)
}


read_multiple_files <- function(list_of_files) {
  df <- list_of_files |>
    map_dfr(read_txt_file)
  return(df)
}


remove_first_two_columns <- function(df) {
  df <- df |>
    select(!c("...1", "...2"))
  return(df)
}

remove_unnecessary_columns <- function(df) {
  # Remove unnecessary columns which are added since 2024-04 data
  df <- df |>
    select(!c("AAG", "MatNr Val. Class"))
}


remove_sub_total_rows <- function(df) {
  df <- df |>
    filter(!is.na(df$RecordType))
  return(df)
}


clean_column_names <- function(df) {
  return(df)
}


main <- function() {
  # Variable
  year <- 2024

  # Path
  data_path <- here(path, "data", "actual")

  # Filenames
  input_files <- dir_ls(data_path, regexp = "\\.TXT")
  output_file <- here(path, "db", glue("COPA_Sales_{year}.parquet"))

  # Read data
  df <- read_multiple_files((input_files))

  # Process data
  df <- df |>
    remove_first_two_columns() |>
    remove_unnecessary_columns() |>  # which are added since 2024-04 data
    remove_sub_total_rows() |>
    clean_names()

  # Write data
  write_parquet(df, output_file)
  print("A parquet file is created")
}

main()
