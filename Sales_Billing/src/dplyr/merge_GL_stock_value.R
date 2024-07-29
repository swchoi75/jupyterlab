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
  )
  return(df)
}


read_multiple_files <- function(list_of_files) {
  df <- list_of_files |>
    map_dfr(read_txt_file)
  return(df)
}


main <- function() {
  # Variables
  year <- "2024"

  # Path
  data_path <- here(path, "data", "GL")

  # Filenames
  input_files <- dir_ls(data_path, regexp = "\\.xls")
  output_file <- here(path, "db", glue("GL_{year}.csv"))

  # Read data
  df <- read_multiple_files((input_files)) |>
    clean_names()

  # Process data
  df <- df |>
    select(!c(1:2, 6)) |>
    filter(!is.na(df$g_l))

  # Write data
  write_csv(df, output_file, na = "")
  print("A file is created")
}

main()
