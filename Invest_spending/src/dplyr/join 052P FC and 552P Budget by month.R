library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(janitor)
library(here)


# Path
path <- here("Investment_spending")


# Functions
read_excel_file <- function(path) {
  # Remove NA rows, Remove "\n", add Version info
  df <- read_excel(path, sheet = "Sheet1") |>
    clean_names() |>
    filter(!is.na(.data$values)) |>
    mutate(items = str_replace_all(.data$items, "\r\n", " "))
  return(df)
}

rename_columns <- function(df) {
  df <- df |>
    rename(c(
      "location" = "location_receiver_5",
      "location_key" = "location_receiver_6",
      "master" = "master_7",
      "master_id" = "master_8"
    ))
  return(df)
}

add_version <- function(df) {
  df <- df |>
    mutate(
      version = str_extract(.data$items, "plan|FC|Actual"),
      version = str_replace(.data$version, "plan", "Budget")
    ) |>
    relocate("version") |>
    arrange(desc("version"))
  return(df)
}

add_month <- function(df) {
  df <- df |>
    mutate(
      month = str_extract(
        .data$items,
        "Jan|Feb|Mar|Apr|May|June|July|Aug|Sep|Oct|Nov|Dec"
      )
    ) |>
    filter(!is.na(.data$month))
  return(df)
}

add_quarter <- function(df) {
  df <- df |>
    mutate(
      Quarter = case_when(
        .data$month %in% c("Jan", "Feb", "Mar") ~ "Q1",
        .data$month %in% c("Apr", "May", "June") ~ "Q2",
        .data$month %in% c("July", "Aug", "Sep") ~ "Q3",
        .data$month %in% c("Oct", "Nov", "Dec") ~ "Q4",
      )
    )
  return(df)
}

process_numeric_columns <- function(df, bud_fx, fc_fx) {
  df <- df |>
    rename(c("k_lc" = "values")) |>
    mutate(
      k_eur = case_when(
        version == "Budget" ~ round(.data$k_lc / bud_fx, 3),
        TRUE ~ round(.data$k_lc / fc_fx, 3)
      ),
      k_eur_at_budget_fx = round(.data$k_lc / bud_fx, 3),
    )
  return(df)
}

join_top_15_projects <- function(df, df_meta) {
  df <- df |>
    left_join(df_meta, by = "master_id") |>
    mutate(
      category = case_when(
        is.na(category) ~ "Other Projects",
        TRUE ~ category
      )
    )
  return(df)
}

main <- function() {
  # Variables
  bud_fx <- 1329 # Budget FX rate in 2023
  fc_fx <- 1411.92300 # YTD October P&L rate (KRW / EUR)

  # Filenames
  input_fc <- here(
    path, "data",
    "052P FC by month/GFW_ICH_V378 FC10+2_KRW.xlsx"
  )
  input_bud <- here(
    path, "data",
    "552P Budget by month/GFW_ICH_V359 Budget 2023_KRW.xlsx"
  )
  meta_file <- here(path, "data", "top 15 projects.csv")
  output_file <- here(path, "output", "Monthly Spending FC10+2.csv")

  # Read data
  df_fc <- read_excel_file(input_fc)
  df_bud <- read_excel_file(input_bud)
  df_top <- read_csv(meta_file, col_select = c(1:2), show_col_types = FALSE) |>
    clean_names()

  # Process data
  df <- bind_rows(df_fc, df_bud)
  df <- df |>
    rename_columns() |>
    add_version() |>
    add_month() |>
    add_quarter() |>
    process_numeric_columns(bud_fx, fc_fx) |>
    join_top_15_projects(df_top)

  # Write data
  write_csv(df, output_file)
  print("A file is created")
}

main()
