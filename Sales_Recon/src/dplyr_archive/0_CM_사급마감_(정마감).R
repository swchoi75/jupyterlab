library(dplyr)
library(readxl)
library(purrr)
library(readr)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Recon")


# Functions
read_excel_multiple_sheets <- function(file_path) {
  # select Excel sheets
  wb_sheets <- excel_sheets(file_path)
  wb_sheets <- wb_sheets[2:7]
  # read multiple excel sheets
  df <- wb_sheets |>
    set_names() |>
    map_df(
      ~ read_xlsx(path = file_path, sheet = .x, skip = 2, col_types = "text"),
    )
  return(df)
}


change_data_type <- function(df) {
  df <- df |>
    mutate(
      across(c("입고누계수량", "포장비누계"), as.double),
    )
  return(df)
}


filter_out_blank <- function(df) {
  df <- df |>
    filter(.data$`Part No` != 0)
  return(df)
}


main <- function() {
  # Variables
  source(here(path, "src", "dplyr", "common_variable.R"))

  # Filenames
  input_file <- here(
    path, "data", "VAN CM", glue("CAE VAN ALL {year}{month}.xlsx")
  )
  meta_file <- here(path, "meta", "사급업체 품번 Master.xlsx")
  output_file <- here(path, "data", "VAN CM", "result.csv")

  # Read data
  df <- read_excel_multiple_sheets(input_file)
  df_meta <- read_xlsx(meta_file, range = "A2:J1000") |>
    filter(!is.na(.data$업체))

  # Process data
  df <- df |>
    change_data_type() |>
    filter_out_blank()

  sub_1 <- df_meta |>
    distinct(pick("Customer P/N", "Mat. Type", "Div.", "업체"))
  sub_2 <- df_meta |>
    distinct(pick("CASCO Part No.", "Mat. Type", "Div.", "업체"))

  ## Join two dataframes
  df_1 <- df |>
    left_join(sub_1, by = c("Part No" = "Customer P/N")) |>
    filter(!is.na(.data$업체)) |>
    arrange(.data$업체) |>
    mutate(구분 = "by Customer PN")

  df_2 <- df |>
    left_join(sub_2, by = c("CAE" = "CASCO Part No.")) |>
    filter(!is.na(.data$업체)) |>
    arrange(.data$업체) |>
    mutate(구분 = "by Material")

  ## Join two dataframes
  df <- bind_rows(df_1, df_2)

  # Write data
  write_excel_csv(df, output_file)
  print("A file is created")
}

main()
