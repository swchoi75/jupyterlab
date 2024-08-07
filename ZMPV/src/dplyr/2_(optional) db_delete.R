library(dplyr)
library(readr)
library(janitor)
library(here)


# Path
path <- here("ZMPV")

# Functions
delete_last_month <- function(df) {
  # Find the last month
  last_month <- last(df$m_y_from)

  # Filter out last month
  df <- df |>
    filter(.data$m_y_from != last_month)

  return(df)
}


main <- function() {
  # Filenames
  db_file <- here(path, "db", "ZMPV_2024.csv")

  # Read data
  df <- read_csv(db_file, show_col_types = FALSE)

  # Process data
  df <- delete_last_month(df)

  # Write data
  write_csv(df, db_file, na = "")
  print("A file is updated")
}

main()
