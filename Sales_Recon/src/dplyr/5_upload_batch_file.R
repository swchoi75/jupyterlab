library(dplyr)
library(readxl)
library(stringr)
library(writexl)
library(readr)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Recon")


# Functions
remove_unnecessary_row <- function(df) {
  df <- df |>
    filter(.data$plant != "0") |>
    filter(.data$order_reason != "NA")
  return(df)
}


change_data_type <- function(df) {
  df <- df |>
    mutate(across(c("quantity", "price"), as.double))
  return(df)
}


change_zero_to_blank <- function(df) {
  # Change 0 back to blank in 이월체크 ----
  df <- df |>
    mutate(
      이월체크 = case_when(
        이월체크 == "0" ~ "",
        TRUE ~ 이월체크
      )
    )
  return(df)
}


main <- function() {
  # Variables
  source(here(path, "src", "dplyr", "common_variable.R"))

  # Filenames
  input_1 <- here(
    path, "report in Excel", glue("{year}-{month}"),
    glue("{year}-{month} Sales adjustment batch_Qty.xlsx")
  )
  input_2 <- here(
    path, "report in Excel", glue("{year}-{month}"),
    glue("{year}-{month} Sales adjustment batch_Price.xlsx")
  )

  output_1 <- here(path, "output", "5. Upload batch file.csv")
  output_2 <- here(path, "output", "5. Upload to be skipped.csv")

  # Read data
  df_qty <- read_xlsx(input_1, sheet = "Format", col_types = "text") |>
    clean_names(ascii = FALSE)
  df_amt <- read_xlsx(input_2, sheet = "Format", col_types = "text") |>
    clean_names(ascii = FALSE)

  # Process data
  df <- bind_rows(df_qty, df_amt)
  df <- df |>
    remove_unnecessary_row() |>
    change_data_type() |>
    change_zero_to_blank()

  ## Data to be deleted
  result <- df |>
    filter(!((.data$order_reason == "C02" & .data$price == 0) |
               .data$material == "0"))

  skip <- df |>
    filter((.data$order_reason == "C02" & .data$price == 0) |
             .data$material == "0")

  # Write data
  write_excel_csv(result, output_1, na = "")
  write_excel_csv(skip, output_2, na = "")
  print("Files are created")
}

main()
