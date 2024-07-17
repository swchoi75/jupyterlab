library(dplyr)
library(readr)
library(stringr)
library(here)


# Path
path <- here("ZX05")


# Functions
read_txt_file <- function(filename) {
  df <- read_tsv(filename, show_col_types = FALSE) |>
    select(c("Cost center", "Act", "Plan", "Tgt")) |>
    rename(c(
      text_col = "Cost center",
      actual = "Act",
      plan = "Plan",
      target = "Tgt"
    ))

  return(df)
}

# Retrieve Cost center and GL accounts columns using Regex (str_extract)
costctr_and_accounts <- function(df, year, month) {
  df <- df |>
    mutate(
      fy = year,
      period = month,
      costctr = str_extract(
        df$text_col,
        "\\s[0-9]{4,5}|IC-.{4,5}|CY-.{4,5}|ICH-.{4,5}|DUMMY_.{3}"
      ) |> str_trim(),
      gl_accounts = str_extract(
        df$text_col,
        "^K[0-9]+|^S[0-9]+"
      )
    ) |>
    tidyr::fill(df$costctr, .direction = "up")

  return(df)
}

filter_and_select <- function(df) {
  df <- df |>
    filter(!(df$actual == 0 & df$plan == 0 &
               df$target == 0)) |>
    select(c(
      "fy", "period", "costctr", "gl_accounts", "actual", "plan", "target"
    ))

  return(df)
}

main <- function() {
  # Variables
  year <- "2024"
  month <- "03" # Monthly to be updated

  # Filenames
  input_file_cf <- here(path, "data", "CF_2024_03.dat")
  input_file_pl <- here(path, "data", "PL_2024_03.dat")

  db_file_cf <- here(path, "db", "CF_2024.csv")
  db_file_pl <- here(path, "db", "PL_2024.csv")

  # Read data
  df_cf <- read_txt_file(input_file_cf)
  df_pl <- read_txt_file(input_file_pl)

  # Process data
  df_cf <- df_cf |>
    costctr_and_accounts(year, month) |>
    filter_and_select()

  df_pl <- df_pl |>
    costctr_and_accounts(year, month) |>
    filter_and_select()

  # Write data
  write_csv(df_cf, db_file_cf, na = "0", append = TRUE)
  write_csv(df_pl, db_file_pl, na = "0", append = TRUE)
  print("Files are updated")
}

main()
