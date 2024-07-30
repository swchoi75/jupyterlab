library(dplyr)
library(readr)
library(stringr)
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
      `Billing Day` = str_sub(`Billing Date`, 9, 10),
      .after = Plant
    )
  return(df)
}


remove_unnecessary_row <- function(df) {
  # Filter out unneccessary rows on 2nd day ----
  df <- df |>
    filter(!(.data$`Billing Day` == "02" &
               !str_detect(.data$`Purchase Order`, "매출조정")
           ))
  return(df)
}


main <- function() {
  # Variables
  year <- "2024"
  month <- "06"

  # Filenames
  input_1 <- here(
    path, "data", "SAP results",
    glue("Sales adjustment_0180_{year}{month}")
  )
  input_2 <- here(
    path, "data", "SAP results",
    glue("Sales adjustment_0180_{year}{month}")
  )
  output_file <- here(path, "output", "6-2. SAP uninvoiced sales.csv")


  # Read data
  df_0180 <- read_txt_file(input_1) |> mutate(Plant = "0180")
  df_2182 <- read_txt_file(input_2) |> mutate(Plant = "2182")

  # Process data
  df <- bind_rows(df_0180, df_2182) |>
    relocate(Plant, .after = `Sales Organization`)

  df <- df |>
    two_billing_dates() |>
    remove_unnecessary_row()

  # Write data
  write_excel_csv(df, output_file)
  print("A file is created")
}

main()
