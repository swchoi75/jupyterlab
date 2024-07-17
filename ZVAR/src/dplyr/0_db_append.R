library(readr)
library(dplyr)
library(janitor)
library(here)


# Path
path <- here("ZVAR")


# Functions
read_txt_file <- function(path) {
  df <- read_tsv(path,
    skip = 9,
    locale = locale(encoding = "UTF-16LE"),
    show_col_types = FALSE
  )
  return(df)
}

main <- function() {
  # Filenames
  input_file <- here(path, "data", "ZVAR_2024_03.txt")
  db_file <- here(path, "db", "ZVAR_2024.csv")

  # Read data
  df <- read_txt_file(input_file)

  # Process data
  # Remove first two columns and sub-total rows
  df <- df |>
    select(-c(1, 2, 5)) |>
    filter(!is.na(df$Order)) |>
    clean_names()


  # Write data
  write_csv(df, db_file, append = TRUE, na = "")
  print("A file is updated")
}

main()
