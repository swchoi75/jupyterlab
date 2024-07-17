library(dplyr)
library(readr)
library(purrr)
library(readxl)
library(fs)
library(stringr)
library(janitor)
library(here)
library(glue)


# Path
path <- here("SPOT")


# Functions
read_multiple_files <- function(list_of_files) {
  df <- list_of_files |>
    map_dfr(read_excel,
      sheet = "Monthly SPOT Overview",
      skip = 9,
      .id = "source"
    )
  return(df)
}

rename_columns_in_2022 <- function(df) {
  # This is used only for SPOT 2022 data
  df <- df |> rename(
    "volume_p_a_2031_st" = "st_42",
    "volume_p_a_2032_st" = "st_43",
    "volume_p_a_overall_result_st" = "st_44"
  )
  return(df)
}


extract_numbers <- function(df) {
  df$source <- str_extract(df$source, "\\d{6}")
  return(df)
}


main <- function() {
  # Variable
  year <- 2024

  # Path
  data_path <- here(path, "data", glue("{year}"))

  # Filenames
  xls_files <- dir_ls(data_path, regexp = "\\.xls?")
  output_file <- here(path, "output", glue("SPOT_combined_{year}.csv"))

  # Read data
  df <- read_multiple_files((xls_files))

  # Process data
  if (year == 2022) {
    df <- df |>
      clean_names() |>
      rename_columns_in_2022() |>
      extract_numbers()
  } else {
    df <- df |>
      clean_names() |>
      extract_numbers()
  }

  # Write data
  write_csv(df, output_file)
  print("A file is created")
}

main()