library(dplyr)
library(readr)
library(stringr)
library(janitor)
library(here)


# Path
path <- here("Planning_Strategy")


# Functions
pivot_data_longer <- function(df) {
  # Tidy the data table into longer format
  df <- df |>
    tidyr::pivot_longer(
      cols = c("sales_2023":"volume_overall_result"),
      names_to = c(".value", "year"),
      names_sep = "_",
    )
  return(df)
}

remove_na_zero <- function(df) {
  df <- df |>
    filter(!.data[["volume"]] == 0) |> # remove zero values
    filter(!str_detect(.data[["year"]], "result")) # remove Result Row
  return(df)
}

extract_year <- function(df) {
  # Regex extract Years in 4 numeric letters
  df <- df |>
    mutate(Year = str_extract(.data[["year"]], "[0-9]{4}"))
  return(df)
}

remove_two_years <- function(df) {
  # Filter out last two year which is strange
  df <- df |>
    filter(!.data[["year"]] == "2031") |>
    filter(!.data[["year"]] == "2032")
  return(df)
}

main <- function() {
  # Filenames
  input_file <- here(path, "data", "SPOT", "2022", "SPOT_combined.csv")
  output_file <- here(path, "output", "SPOT merged.csv")

  # Read data
  df <- read_csv(input_file, show_col_types = FALSE)

  # Process data
  df <- df |>
    clean_names() |>
    pivot_data_longer() |>
    remove_na_zero() |>
    extract_year() |>
    remove_two_years()

  # Write data
  write_csv(df, output_file)
  print("A file is created")
}

main()
