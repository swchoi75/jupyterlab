library(dplyr)
library(readr)
library(stringr)
library(readxl)
library(janitor)
library(here)


# Path
path <- here("PnL")


# Functions
read_excel_file <- function(filename, version) {
  df <- read_excel(filename, sheet = "Query", skip = 11) |>
    rename(c(
      `FS item description` = "...2",
      `CU name` = "...4",
      `Plant name` = "...6",
      `Outlet name` = "...8",
      `YTD PM` = "YTD - 1"
    )) |>
    rename_with(~ gsub(version, "", .x)) |>
    clean_names()
  return(df)
}

process_currency <- function(df, lc_gc) {
  df <- df |>
    mutate(currency = lc_gc) |>
    relocate(c("currency"))
  return(df)
}

outlet <- function(filename) {
  # POC
  col_poc <- c("division", "bu", "new_outlet", "new_outlet_name")

  df <- read_excel(filename, range = cell_cols("A:F")) |>
    clean_names() |>
    select(!c("outlet_name")) |>
    select("outlet", all_of(col_poc))
  return(df)
}

join_with_outlet <- function(df, filename) {
  df <- df |>
    left_join(outlet(filename), by = "outlet")
  return(df)
}

profit_and_loss <- function(df) {
  df <- df |>
    select(!c("period_0", "ytd_0":"ytd_12")) |>
    filter(str_detect(.data[["financial_statement_item"]], "^3|^CO"))
  return(df)
}

balance_sheet <- function(df) {
  df <- df |>
    select(!c("period_0":"period_12")) |>
    filter(str_detect(.data[["financial_statement_item"]], "^1|^2"))
  return(df)
}

main <- function() {
  # Variables
  version <- "\r\nACT"

  # Filenames
  input_lc <- here(
    path, "data", "RACE",
    "Analysis FS Item Hierarchy for CU 698_LC.xlsx"
  )
  input_gc <- here(
    path, "data", "RACE",
    "Analysis FS Item Hierarchy for CU 698_GC.xlsx"
  )
  meta_file <- here(path, "meta", "New outlet.xlsx")
  output_pnl <- here(path, "output", "RACE Profit and Loss.csv")
  output_bs <- here(path, "output", "RACE Balance sheet.csv")


  # Combine data
  lc <- read_excel_file(input_lc, version) |> process_currency("LC")
  gc <- read_excel_file(input_gc, version) |> process_currency("GC")
  race <- bind_rows(lc, gc) |> join_with_outlet(meta_file)

  # Split data
  race_pnl <- profit_and_loss(race)
  race_bs <- balance_sheet(race)

  # Output data
  write_csv(race_pnl, output_pnl, na = "0")
  write_csv(race_bs, output_bs, na = "0")
}

main()
