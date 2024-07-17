library(dplyr)
library(readr)
library(janitor)
library(here)


# Path
path <- here("ZMPV")


# Functions
read_txt_file <- function(path) {
  df <- read_tsv(path,
    skip = 7,
    locale = locale(encoding = "UTF-16LE"),
    col_types = "ccc",
  )
  return(df)
}

remove_col_row <- function(df) {
  # Remove first two columns and sub-total rows
  df <- df |>
    select(-c(1, 2)) |>
    filter(!is.na(df["Profit Cen"]))
  return(df)
}

convert_col_type <- function(df) {
  df <- df |>
    mutate(
      across(c(
        "M/Y (from-",
        "Trading Pr",
        "Accounts f",
        "Document d"
      ), as.character)
    )
  return(df)
}

rename_cols <- function(df) {
  lookup <- c(
    `Net PPV` = "Net PM_PPV...23",
    `Net PPV ratio` = "Net PM_PPV...24",
    `STD Other` = "STD Other...39",
    `STD Other 2` = "STD Other...40"
  )
  df <- df |>
    rename(all_of(lookup))
  return(df)
}

main <- function() {
  # Filenames
  input_file <- here(path, "data", "ZMPV_2024_03.txt")
  db_file <- here(path, "db", "ZMPV_2024.csv")

  # Read data
  df <- read_txt_file(input_file)

  # Process data
  df <- df |>
    remove_col_row() |>
    convert_col_type() |>
    rename_cols() |>
    clean_names()

  # Write data
  write_csv(df, db_file, append = TRUE, na = "")
  print("A file is updated")
}

main()
