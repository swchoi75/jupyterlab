library(readr)
library(dplyr)

db_append <- function(filename) {
  # Read a tab-delimited file ----
  df <- read_tsv(filename,
    skip = 10,
    locale = locale(encoding = "UTF-16LE"),
    col_types = cols(`RecordType` = col_character()),
  )

  # Remove first two columns and sub-total rows ----
  df <- df |>
    select(-c(1, 2)) |>
    filter(!is.na(df$RecordType))

  return(df)
}

db_delete <- function(path) {
  # Read a data file in CSV format
  df <- read_csv(path, show_col_types = FALSE)

  # Find the last month
  last_month <- last(df$"Period")

  # Filter out last month
  df <- df |>
    filter(df$Period != last_month)

  return(df)
}
