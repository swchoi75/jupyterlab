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
      sheet = "Seasonalized Variations",
      range = "A16:Y252",
      .id = "source"
    )
  return(df)
}

process_columns <- function(df) {
  # Select columns and remove NA rows
  df <- df |>
    select(-c(4:6, 24:26)) |>
    rename(c(
      Plant  = "...1",
      Outlet = "...2",
      Items  = "...6"
    ))
  return(df)
}

filter_rows <- function(df) {
  df <- df |>
    filter(!is.na(.data[["plant"]]))
  return(df)
}

process_text <- function(df, path_name) {
  # Remove unnecessary text
  df <- df |>
    mutate(
      source = str_remove(.data[["source"]], "_2023"),
      source = str_remove(.data[["source"]], ".xlsx"),
      source = str_remove(.data[["source"]], path_name)
    )
  return(df)
}

poc <- function(filename) {
  df <- read_csv(filename, show_col_types = FALSE) |> clean_names()
  return(df)
}

join_with_poc <- function(df, filename) {
  # Add POC and Variations item information
  df <- df |>
    left_join(poc(filename), by = c("plant", "outlet"))
  return(df)
}

variations_item <- function(filename) {
  df <- read_csv(filename, show_col_types = FALSE) |> clean_names()
  return(df)
}

join_with_variations <- function(df, filename) {
  df <- df |>
    left_join(variations_item(filename), by = "items")
  return(df)
}

main <- function() {
  # Variables
  path_name <- paste0(
    path, "/data/Variations tracking/variations_tracking_ICH_"
  )

  # Path
  data_path <- here(path, "data", "Variations tracking")

  # Filenames
  xls_files <- dir_ls(data_path, regexp = "\\.xlsx$")
  meta_poc <- here(path, "meta", "POC.csv")
  meta_file <- here(path, "meta", "Variations items.csv")
  output_file <- here(path, "output", "Variations trackings.csv")

  # Read data
  df <- read_multiple_files(xls_files)

  # Process data
  df <- df |>
    process_columns() |>
    clean_names() |>
    filter_rows() |>
    process_text(path_name) |>
    join_with_poc(meta_poc) |>
    join_with_variations(meta_file)

  # Write data
  write_csv(df, output_file)
  print("A file is created")
}

main()
