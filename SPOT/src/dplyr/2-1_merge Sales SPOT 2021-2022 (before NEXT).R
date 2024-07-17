library(dplyr)
library(readr)
library(stringr)
library(janitor)
library(here)
library(glue)


# Path
path <- here("SPOT")


# Functions
pivot_data_longer <- function(df, list_of_columns) {
  # Tidy the data table into longer format
  df <- df |>
    tidyr::pivot_longer(
      # cols in R is opposite of id_vars in Python
      cols = -c(all_of(list_of_columns)),
      names_to = "key",
      values_to = "values",
    )
  # Extract text from key column and create new columns
  df$year <- str_extract(df$key, "\\d{4}")
  df$measure <- str_extract(df$key, "sales|volume|price")
  return(df)
}

pivot_data_wider <- function(df, list_of_columns) {
  df <- df |>
    tidyr::pivot_wider(
      id_cols = c(all_of(list_of_columns)),
      names_from = "measure",
      values_from = "values",
      values_fn = sum,
    )
  return(df)
}

remove_zero_na <- function(df) {
  df <- df |>
    # remove zero values
    filter(!(df$sales == 0 & df$volume == 0)) |>
    # remove na values
    tidyr::drop_na("year")
  return(df)
}

main <- function() {
  # Variable
  year <- 2022

  list_of_cols <- c(
    "source",
    "business_unit",
    "segment", # before project NEXT
    "business_type",
    "om_status",
    "sourcing_customer",
    "sourcing_decis_date",
    "project_title",
    "project_id",
    "line_item_descr", # before project NEXT
    "sop_line_item",
    "won_lost_exit_date"
  )

  # Filenames
  input_file <- here(path, "output", glue("SPOT_combined_{year}.csv"))
  output_file <- here(path, "output", glue("SPOT_merged_{year}.csv"))

  # Read data
  df <- read_csv(input_file, show_col_types = FALSE)

  # Process data
  df <- df |>
    pivot_data_longer(list_of_cols) |>
    pivot_data_wider(c(list_of_cols, "year")) |>  # exclude "key" column
    remove_zero_na()

  # Write data
  write_csv(df, output_file)
  print("A file is created")
}

main()
