library(readr)
library(dplyr)
library(janitor)
library(here)


# Path
path <- here("ZVAR")

# Functions
delete_last_month <- function(path) {
  # Read a data file in CSV format
  df <- read_csv(path, show_col_types = FALSE)

  # Find the last month
  last_month <- last(df$"per")

  # Filter out last month
  df <- df |>
    filter(.data$per != last_month)

  return(df)
}

main <- function() {
  # Filenames
  db_file <- here(path, "db", "ZVAR_2024.csv")

  # Process data
  df <- delete_last_month(db_file)

  # Write data
  write_csv(df, db_file, na = "")
  print("A file is updated")
}

main()
