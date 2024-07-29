library(dplyr)
library(arrow)
library(readxl)
library(readr)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_P3")


# Functions
drop_columns <- function(df) {
  df <- df |>
    select(!c(
      "doc_no",
      "created_on",
      "created_by",
      "ref_doc_no",
      "rf_itm",
      "ac_document_no",
      "delivery",
      "item",
      "tr_prt"
    ))
  return(df)
}


group_summarize <- function(df) {
  group_cols <- setdiff(names(df), c("quantity", "totsaleslc", "stock_value"))
  df <- df |>
    group_by(across(all_of(group_cols))) |>
    summarise(
      quantity = sum(df$quantity),
      tot_sales_lc = sum(df$tot_sales_lc),
      stock_value = sum(df$stock_value)
    ) |>
    ungroup()
  return(df)
}


rename_columns <- function(df) {
  df <- df |>
    rename(c(
      "qty" = "quantity",
      "sales_lc" = "tot_sales_lc",
      "std_costs" = "stock_value",
      "recordtype" = "record_type",
      "material_description" = "mat_nr_descr"
    ))
  return(df)
}


filter_out_zero <- function(df) {
  df <- df |>
    filter(!(
      df$qty == 0 &
        df$sales_lc == 0 &
        df$std_costs == 0
    ))
  return(df)
}


split_period <- function(df) {
  # Split Period into Year / Month
  df <- df |>
    tidyr::separate(
      "period",
      into = c("year", "month"),
      sep = "\\."
    ) |>
    mutate(across(c("year":"month"), as.double))
  return(df)
}


process_std_costs <- function(df) {
  df <- df |>
    mutate(std_costs = ifelse(df["d_c"] == 40,
      df$std_costs,
      df$std_costs * -1
    )) |>
    select(!c("d_c"))
  return(df)
}


main <- function() {
  # Variable
  year <- 2024

  # Filenames
  input_main <- here(path, "db", glue("COPA_Sales_{year}.parquet"))
  input_sub <- here(path, "data", "actual", "Kappa HEV adj_costs.xlsx")
  output_file <- here(path, "output", "1_actual_sales.csv")

  # Read data
  df <- read_parquet(input_main)
  df_sub <- read_excel(input_sub, sheet = "format")

  # Process data
  df <- df |>
    drop_columns() |>
    group_summarize() |>
    rename_columns() |>
    filter_out_zero() |>
    split_period()

  kappa_cost <- df_sub |> clean_names() # |> process_std_costs()

  df <- bind_rows(df, kappa_cost) # Add Kappa HEV adjustment (Costs)

  # Write data
  write_csv(df, output_file)
  print("A file is created")
}

main()
