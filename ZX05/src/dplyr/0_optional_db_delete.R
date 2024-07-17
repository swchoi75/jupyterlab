library(dplyr)
library(readr)
library(here)


# Path
path <- here("ZX05")


# Functions
delete_last_month <- function(df) {
  # Find the last month
  last_month <- last(df$"period")

  # Filter out last month
  df <- df |>
    filter(df$period != last_month)

  return(df)
}

main <- function() {
  # Filenames
  db_file_cf <- here(path, "db", "CF_2024.csv")
  db_file_pl <- here(path, "db", "PL_2024.csv")

  # Read data
  df_cf <- read_csv(db_file_cf, show_col_types = FALSE)
  df_pl <- read_csv(db_file_pl, show_col_types = FALSE)

  # Process data
  df_cf <- delete_last_month(df_cf)
  df_pl <- delete_last_month(df_pl)

  # Write data
  write_csv(df_cf, db_file_cf, na = "")
  write_csv(df_pl, db_file_pl, na = "")
  print("Files are updated")
}

main()
