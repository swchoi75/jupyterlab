library(dplyr)
library(readr)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Billing")


# Functions
read_txt_file <- function(file_path) {
  df <- read_tsv(file_path,
    skip = 3,
    locale = locale(encoding = "UTF-16LE"),
    col_types = cols(.default = col_character()),
  ) |>
    clean_names() |>
    select(!c(1:2)) |>
    filter(!is.na(.data$sales_org))
  return(df)
}


filter_price_missing <- function(df) {
  df <- df |>
    filter(.data$delivery_amount == 0) |>
    select(c(
      "sold_to_party",
      "customer_material",
      "material_number",
      "material_description",
      "profit_center",
      "delivery_date",
      "mvt_type",
      "quantity",
      "delivery_amount",
      "delivery_number",
      "billing_number",
      "billing_amount",
      "item"
    ))
  return(df)
}


main <- function() {
  # Variables
  year <- "2024"
  month <- "07"

  # Filenames
  input_1 <- here(
    path, "Sales Delivery Report",
    glue("Delivery report_0180_{year}_{month}.xls")
  )
  input_2 <- here(
    path, "Sales Delivery Report",
    glue("Delivery report_2182_{year}_{month}.xls")
  )
  output_1 <- here(path, "output", "Sales delivery report.csv")
  output_2 <- here(path, "output", "Sales price missing.csv")

  # Read data
  df_0180 <- read_txt_file(input_1)
  df_2182 <- read_txt_file(input_2)

  # Process data
  df <- bind_rows(df_0180, df_2182)
  df_sub <- filter_price_missing((df))

  # Write data
  write_csv(df, output_1, na = "")
  write_csv(df_sub, output_2, na = "")
  print("Files are created")
}

main()
