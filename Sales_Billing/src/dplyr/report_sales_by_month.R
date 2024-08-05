library(dplyr)
library(readr)
library(here)


# Path
path <- here("Sales_Billing")


# Functions
rename_columns <- function(df) {
  colnames(df) <- c(
    "prctr", "product_hier", "customer", "material", "description",
    "platform", "division", "customer_part_no", "material_type", "std_cost",
    "01_qty", "01_sum", "02_qty", "02_sum", "03_qty", "03_sum",
    "04_qty", "04_sum", "05_qty", "05_sum", "06_qty", "06_sum",
    "07_qty", "07_sum", "08_qty", "08_sum", "09_qty", "09_sum",
    "10_qty", "10_sum", "11_qty", "11_sum", "12_qty", "12_sum",
    "total_qty", "total_sum"
  )
  return(df)
}


overwrite_with_blank_data <- function(df) {
  df <- df |>
    mutate(std_cost = "")  # Make std_cost into blank
  return(df)
}


remove_missing_values <- function(df) {
  df <- df |>
    filter(!is.na(.data$prctr))
  return(df)
}


main <- function() {
  # Filenames
  input_file <- here(path, "data", "Sales_20240702.xls")
  output_file <- here(path, "output", "Sales by month.csv")

  # Read data
  df <- read_tsv(input_file, show_col_types = FALSE)

  # Process data
  df <- df |>
    rename_columns() |>
    overwrite_with_blank_data() |>
    remove_missing_values()

  # Write data
  write_csv(df, output_file, na = "")
  print("A file is created")
}

main()