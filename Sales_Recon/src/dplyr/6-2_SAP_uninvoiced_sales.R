library(dplyr)
library(readr)
library(stringr)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Recon")


# Functions
read_txt_file <- function(file_path) {
  df <- read_tsv(file_path,
    skip = 4,
    locale = locale(encoding = "UTF-16LE"),
    col_types = cols(.default = col_character()),
  ) |>
    filter(!is.na(.data$`Sales Organization`)) |>
    select(-c(1, 2, 23))
  return(df)
}


two_billing_dates <- function(df) {
  df <- df |>
    mutate(
      billing_day = str_sub(.data$billing_date, 9, 10),
      .after = "plant"
    )
  return(df)
}


remove_unnecessary_row <- function(df) {
  # Filter out unneccessary rows on 2nd day ----
  df <- df |>
    filter(!(.data$billing_day == "02" &
               !str_detect(.data$purchase_order, "매출조정")
           ))
  return(df)
}


main <- function() {
  # Variables
  source(here(path, "src", "dplyr", "common_variable.R"))

  # Filenames
  input_1 <- here(
    path, "data", "SAP_results",
    glue("Sales adjustment_0180_{year}{month}.xls")
  )
  input_2 <- here(
    path, "data", "SAP_results",
    glue("Sales adjustment_2182_{year}{month}.xls")
  )
  output_file <- here(path, "output", "6-2. SAP uninvoiced sales.csv")


  # Read data
  df_0180 <- read_txt_file(input_1) |>
    clean_names() |>
    mutate(plant = "0180")
  
  df_2182 <- read_txt_file(input_2) |>
    clean_names() |>
    mutate(plant = "2182")

  # Process data
  df <- bind_rows(df_0180, df_2182) |>
    relocate("plant", .after = "sales_organization")

  df <- df |>
    two_billing_dates() |>
    remove_unnecessary_row()

  # Write data
  write_excel_csv(df, output_file)
  print("A file is created")
}

main()
