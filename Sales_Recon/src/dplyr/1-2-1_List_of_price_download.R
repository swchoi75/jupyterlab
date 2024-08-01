library(dplyr)
library(readr)
library(stringr)
library(readxl)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Recon")


# Functions
main <- function() {
  # Variables
  source(here(path, "src", "dplyr", "common_variable.R"))

  # Filenames
  input_1 <- here(
    path, "data", "SAP", glue("Billing_0180_{year}_{month}.xlsx")
  )
  input_2 <- here(
    path, "data", "SAP", glue("Billing_2182_{year}_{month}.xlsx")
  )
  output_file <- here(path, "output", "1-2-1. list of price download.xls")

  # Read data
  df_0180 <- read_excel(input_1) |>
    clean_names(ascii = FALSE) |>
    filter(!is.na(.data$sales_organization))

  df_2182 <- read_excel(input_2) |>
    clean_names(ascii = FALSE) |>
    filter(!is.na(.data$sales_organization))

  # Process data
  df <- bind_rows(df_0180, df_2182)

  df <- df |>
    select(c("material_number")) |>
    distinct(pick("material_number"))

  # Write data
  write_tsv(df, output_file, na = "")
  print("A file is created")
}

main()
