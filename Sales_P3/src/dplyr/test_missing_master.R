library(dplyr)
library(readxl)
library(readr)
library(janitor)
library(here)


# Path
path <- here("Sales_P3")


# Functions
missing_customer_center <- function(df) {
  df <- df |>
    select(c("sold_to_party", "sold_to_name_1", "customer_center")) |>
    tidyr::drop_na(c("sold_to_name_1")) |>
    filter(is.na(.data$customer_center)) |>
    distinct()
  return(df)
}


missing_gl_accounts <- function(df) {
  df <- df |>
    select(c("cost_elem", "account_class", "g_l_account_name")) |>
    tidyr::drop_na(c("cost_elem")) |>
    filter(is.na(.data$g_l_account_name)) |>
    distinct()
  return(df)
}


missing_poc <- function(df) {
  df <- df |>
    select(c("profit_ctr", "outlet")) |>
    tidyr::drop_na(c("profit_ctr")) |>
    filter(is.na(.data$outlet)) |>
    distinct()
  return(df)
}


missing_cm_cluster <- function(df, cm_profit_ctr) {
  df <- df |>
    select(c(
      "version", "month", "profit_ctr", "product", "material_type", "cm_cluster"
    )) |>
    tidyr::drop_na(c("product")) |>
    filter(is.na(.data$cm_cluster)) |>
    distinct()

  # filter contract manufacturing product
  df <- df |>
    filter(.data$profit_ctr %in% cm_profit_ctr) |>
    filter(.data$material_type == "FERT")

  return(df)
}


missing_customer_material <- function(df) {
  df <- df |>
    select(c(
      "version", "division", "material_type", "product", "customer_material"
    )) |>
    tidyr::drop_na(c("product")) |>
    filter(is.na(.data$customer_material)) |>
    distinct()
  return(df)
}


missing_material_master <- function(df) {
  df <- df |>
    select(c("version", "year", "profit_ctr", "product", "material_type")) |>
    tidyr::drop_na(c("product")) |>
    filter(is.na(.data$material_type)) |>
    distinct()
  return(df)
}



# filter missing Product hierarchy except CM profit centers
missing_product_hierarchy <- function(df, cm_profit_ctr) {
  df <- df |>
    select(c("profit_ctr", "product_hierarchy", "ph_3_simple")) |>
    filter(is.na(.data$ph_3_simple)) |>
    distinct()

  # filter out contract manufacturing product
  df <- df |>
    filter(!.data$profit_ctr %in% cm_profit_ctr)

  return(df)
}


main <- function() {
  # Variable
  cm_profit_ctr <- c("50803-045", "50803-044", "50803-046")

  # Filenames
  input_file <- here(path, "output", "5_sales_with_master_data.csv")

  output_1 <- here(path, "meta", "test_CC_2024_missing.csv")
  output_2 <- here(path, "meta", "test_GL_missing.csv")
  output_3 <- here(path, "meta", "test_POC_missing.csv")
  output_4 <- here(path, "meta", "test_CM_cluster_missing.csv")
  output_5 <- here(path, "meta", "test_customer_material_missing.csv")
  output_6 <- here(path, "meta", "test_material_master_missing.csv")
  output_7 <- here(path, "meta", "test_PH_info_missing.csv")

  # Read data
  df <- read_csv(input_file, show_col_types = FALSE)

  # Process data
  df_1 <- missing_customer_center(df)
  df_2 <- missing_gl_accounts(df)
  df_3 <- missing_poc(df)
  df_4 <- missing_cm_cluster(df, cm_profit_ctr)
  df_5 <- missing_customer_material(df)
  df_6 <- missing_material_master(df)
  df_7 <- missing_product_hierarchy(df, cm_profit_ctr)


  # Write data
  write_csv(df_1, output_1)
  write_csv(df_2, output_2)
  write_csv(df_3, output_3)
  write_csv(df_4, output_4)
  write_csv(df_5, output_5)
  write_csv(df_6, output_6)
  write_csv(df_7, output_7)

  print("Files are created")
}

main()
