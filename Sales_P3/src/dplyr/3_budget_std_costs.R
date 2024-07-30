library(dplyr)
library(readxl)
library(readr)
library(janitor)
library(here)


# Path
path <- here("Sales_P3")


# Functions
rename_columns <- function(df) {
  df <- df |>
    rename(c(
      "product_hierarchy" = "Product Hierachy...8",
      "product" = "Material",
      "profit_ctr" = "Profit Center"
    ))
  return(df)
}


select_columns <- function(df, list_of_cols) {
  df <- df |>
    select(c(list_of_cols))
  return(df)
}


budget_std_costs <- function(df) {
  df <- df |>
    mutate(std_costs = .data$qty * .data$standard_price) |>
    select(!c("standard_price"))
  return(df)
}


main <- function() {
  # Filenames
  input_file <- here(path, "output", "2_budget_sales.csv")
  meta_1 <- here(path, "meta", "Material master_0180.xlsx")
  meta_2 <- here(path, "meta", "Material master_2182.xlsx")
  output_file <- here(path, "output", "3_budget_std_costs.csv")
  output_meta <- here(path, "meta", "material_master.csv")

  # Read data
  df <- read_csv(input_file, show_col_types = FALSE)
  df_0180 <- read_excel(meta_1, sheet = "MM")
  df_2182 <- read_excel(meta_2, sheet = "MM")

  # Process data
  df_meta <- bind_rows(df_0180, df_2182) |>
    rename_columns() |>
    clean_names() |>
    select_columns(c(
      "product",
      "profit_ctr",
      "material_type",
      "product_hierarchy",
      "standard_price"
    ))

  df_sub <- df_meta |> select(!c("product_hierarchy"))

  df <- df |>
    left_join(df_sub, by = c("profit_ctr", "product")) |>
    budget_std_costs()


  # Write data
  write_csv(df, output_file)
  write_csv(df_meta, output_meta)
  print("Files are created")
}

main()
