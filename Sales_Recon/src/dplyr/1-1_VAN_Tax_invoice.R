library(dplyr)
library(readxl)
library(purrr)
library(stringr)
library(readr)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Recon")


# Functions
read_excel_multiple_sheets <- function(file_path) {
  # select Excel sheets
  wb_sheets <- excel_sheets(file_path)
  wb_sheets <- wb_sheets[1:23]
  # read multiple excel sheets
  df <- wb_sheets |>
    set_names() |>
    map_df(
      ~ read_xlsx(
        path = file_path,
        sheet = .x,
        skip = 2,
        range = "A3:P2000",
        col_types = "text"
      ),
    )
  return(df)
}


remove_unnecessary_rows <- function(df) {
  df <- df |>
    filter(.data$customer_pn != 0) |>
    filter(str_detect(.data$customer_pn, "^[a-zA-Z0-9-_\\s]*$"))
  return(df)
}


change_data_type <- function(df) {
  # Convert types from text to double ----
  columns_to_change <- c(
    "입고수량",
    "입고금액",
    "포장비",
    "단가소급",
    "관세정산",
    "sample",
    "glovis_price",
    "서열비"
  )
  df <- df |>
    mutate(
      across(all_of(columns_to_change), as.double),
      across(all_of(columns_to_change), ~ tidyr::replace_na(.x, 0)),
    )
  return(df)
}


main <- function() {
  # Variables
  source(here(path, "src", "dplyr", "common_variable.R"))

  # Filenames
  input_file <- here(
    path, "data", "VAN VT", glue("{year}{month}"),
    glue("_{year}{month} Tax invoice.xlsx")
  )
  output_file <- here(path, "output", "1-1. Tax invoice all.csv")

  # Read data
  df <- read_excel_multiple_sheets(input_file) |>
    clean_names(ascii = FALSE)

  # Process data
  df <- df |>
    remove_unnecessary_rows() |>
    change_data_type()

  # Write data
  write_excel_csv(df, output_file)
  print("A file is created")
}

main()
