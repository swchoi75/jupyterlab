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


main <- function() {
  # Filenames
  input_file <- here(path, "db", "Sales_20240702.xls")
  output_file <- here(path, "output", "Sales by month.csv")

  # Read data
  df <- read_tsv(input_file, show_col_types = FALSE)

  # Process data
  df <- df |>
    rename_columns() |>
    mutate(std_cost = "") |>  # Make Std.cost into blank
    filter(.data$prctr != "50803-003")

  # Write data
  write_csv(df, output_file, na = "")
  print("A file is created")
}

main()