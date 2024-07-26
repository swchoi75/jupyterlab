library(dplyr)
library(readxl)
library(readr)
library(janitor)
library(here)


# Path
path <- here("Sales_P3")


# Functions
join_customer_center <- function(df, df_meta) {
  df_meta <- df_meta |> clean_names()
  df <- df |>
    left_join(df_meta, by = "sold_to_party")
  return(df)
}

join_gl_accounts <- function(df, df_meta) {
  df_meta <- df_meta |> clean_names()
  df <- df |>
    left_join(df_meta, by = c("cost_elem" = "g_l_account"))
  return(df)
}

join_with_poc <- function(df, df_meta) {
  df_meta <- df_meta |> clean_names()
  df <- df |>
    left_join(df_meta, by = c("profit_ctr" = "profit_center"))
  return(df)
}

join_cm_cluster <- function(df, df_meta) {
  df_meta <- df_meta |> clean_names()
  df <- df |>
    left_join(df_meta, by = c("product" = "material"))
  return(df)
}

join_customer_material <- function(df, df_meta) {
  df_meta <- df_meta |> clean_names()
  df <- df |>
    left_join(df_meta, by = "product")
  return(df)
}

join_material_master <- function(df, df_meta) {
  df_meta <- df_meta |> clean_names()
  df <- df |>
    left_join(df_meta, by = c("profit_ctr", "product"))
  return(df)
}

join_product_hierarchy <- function(df, df_meta) {
  df_meta <- df_meta |> clean_names()
  df <- df |>
    left_join(df_meta, by = c("profit_ctr", "product_hierarchy"))
  return(df)
}


main <- function() {
  # Filenames
  input_file <- here(path, "output", "4_actual_and_budget_sales.csv")
  meta_1 <- here(path, "meta", "CC_2024.csv")
  meta_2 <- here(path, "meta", "GL.csv")
  meta_3 <- here(path, "meta", "POC.csv")
  meta_4 <- here(path, "meta", "2024-06_YPC1 costing_Icheon.xlsx")
  meta_5 <- here(path, "meta", "Customer Material.xlsx")
  meta_6 <- here(path, "meta", "material_master.csv")
  meta_7 <- here(path, "meta", "PH info.csv")
  output_file <- here(path, "output", "5_sales_with_master_data.csv")

  # Read data
  df <- read_csv(input_file, show_col_types = FALSE)
  df_cc <- read_csv(meta_1, show_col_types = FALSE)
  df_gl <- read_csv(meta_2, show_col_types = FALSE)
  df_poc <- read_csv(meta_3, show_col_types = FALSE)
  df_cm_cluster <- read_excel(meta_4, sheet = "YPC1")
  df_cust_mat <- read_excel(meta_5, sheet = "Sheet1")
  df_mat <- read_csv(meta_6, show_col_types = FALSE)
  df_ph <- read_csv(meta_7, show_col_types = FALSE)

  # Process data
  df_cc <- df_cc |>
    select(c("Sold-to party", "Customer Center"))

  df_gl <- df_gl |>
    select(c("G/L account", "G/L account name"))

  df_poc <- df_poc |>
    select(!c("CU"))

  df_cm_cluster <- df_cm_cluster |>
    select(c("Material", "CM Cluster"))

  df_cust_mat <- df_cust_mat |>
    select(c("Product", "Customer Material")) |>
    filter(.data$`Customer Material` != "NA")

  df_ph <- df_ph |>
    rename(c("Profit Ctr" = "Profit Center")) |>
    select(c("Profit Ctr", "Product Hierarchy", "PH_3 simple", "PRD/MER"))

  df <- df |>
    join_customer_center(df_cc) |>
    join_gl_accounts(df_gl) |>
    join_with_poc(df_poc) |>
    join_cm_cluster(df_cm_cluster) |>
    join_customer_material(df_cust_mat) |>
    join_material_master(df_mat) |>
    join_product_hierarchy(df_ph)

  # Write data
  write_csv(df, output_file)
  print("A files is created")
}

main()
