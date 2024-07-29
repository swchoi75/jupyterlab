library(dplyr)
library(readxl)
library(readr)
library(janitor)
library(here)


# Path
path <- here("Sales_P3")


# Functions
bud_price <- function(df, col_name) {
  # Define a custom function to calculate the budget price
  col <- col_name
  df <- df |>
    select(c("version", "profit_ctr",  col, "qty", "sales_lc")) |>
    filter(.data$version == "Budget") |>
    group_by(pick("version", "profit_ctr", col)) |>
    summarise(
      qty = sum(.data$qty),
      sales_lc = sum(.data$sales_lc),
      bud_price = round(.data$sales_lc / .data$qty, 0),
    ) |>
    ungroup() |>
    tidyr::drop_na(col) |>
    select(c("profit_ctr", col, "bud_price")) |>
    mutate(mapping_key = .data[[col]])

  return(df)
}


# 3 different groups ----
select_div_e <- function(df) {
  df <- df |>
    filter(.data$division == "E")
  return(df)
}


select_div_p <- function(df) {
  df <- df |>
    filter(.data$outlet_name %in% c("PL EAC", "PL HYD", "PL MES", "PL DAC E"))
  return(df)
}


select_pl_cm <- function(df) {
  df <- df |>
    filter(.data$outlet_name %in% c("PL CM CCN", "PL CM CVS", "PL CM PSS"))
  return(df)
}


sales_ytd <- function(df) {
  df_act <- df |>
    filter(.data$version == "Actual")
  last_month <- last(df_act$month)
  # Filter YTD in Budget data
  df |>
    filter(df$month <= last_month)
}


spv_mapping <- function(df, bud_price, col_name) {
  # mapping between between actual and budget
  df <- df |>
    left_join(bud_price, by = c("profit_ctr", col_name))
  return(df)
}


main <- function() {
  # Filenames
  input_file <- here(path, "output", "5_sales_with_master_data.csv")
  output_1 <- here(path, "output", "bud_price_div_e.csv")
  output_2 <- here(path, "output", "bud_price_div_p.csv")
  output_3 <- here(path, "output", "bud_price_pl_cm.csv")
  output_file <- here(path, "output", "6_ytd_sales_spv_mapping.csv")

  # Read data
  df <- read_csv(input_file, show_col_types = FALSE)

  # Process data
  ## create budget price tables for 3 different mapping methods
  df_1 <- df |> select_div_e() |> bud_price("product_hierarchy")
  df_2 <- df |> select_div_p() |> bud_price("product")
  df_3 <- df |> select_pl_cm() |> bud_price("cm_cluster")

  ## derive YTD sales
  df_ytd <- sales_ytd(df)

  ## Sales P3: SPV mapping for 3 different scenarios
  df_ytd_act <- df_ytd |> filter(.data$version == "Actual")
  df_ytd_bud <- df_ytd |> filter(.data$version == "Budget")

  map_div_e <- spv_mapping(select_div_e(df_ytd_act), df_1, "product_hierarchy")
  map_div_p <- spv_mapping(select_div_p(df_ytd_act), df_2, "product")
  map_pl_cm <- spv_mapping(select_pl_cm(df_ytd_act), df_3, "cm_cluster")

  df_result <- bind_rows(map_div_e, map_div_p, map_pl_cm, df_ytd_bud)

  # Write data
  write_csv(df_1, output_1)
  write_csv(df_2, output_2)
  write_csv(df_3, output_3)
  write_csv(df_result, output_file)

  print("Files are created")
}

main()