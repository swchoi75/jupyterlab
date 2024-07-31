library(dplyr)
source("script/master_data.R")


# Join main data with meta data ----
join_customer_center <- function(df) {
  # Read meta data in CSV files ----
  cc <- master_cc() |>
    select(c("Sold-to party", "Customer Center"))

  df <- df |>
    left_join(cc, by = "Sold-to party")
  return(df)
}

join_gl_accounts <- function(df) {
  gl <- master_gl() |>
    select(c("G/L account", "G/L account name"))

  df <- df |>
    left_join(gl, by = c("Cost Elem." = "G/L account"))
  return(df)
}

join_with_poc <- function(df) {
  poc <- master_poc() |>
    select(!c("CU"))

  df <- df |>
    left_join(poc, by = c("Profit Ctr" = "Profit Center"))
  return(df)
}

join_cm_cluster <- function(df) {
  cm_cluster <- master_cm_cluster() |>
    select(c("Material", "CM Cluster"))

  df <- df |>
    left_join(cm_cluster, by = c("Product" = "Material"))
  return(df)
}

join_customer_material <- function(df) {
  cust_mat <- customer_material()

  df <- df |>
    left_join(cust_mat, by = "Product")
  return(df)
}

join_material_master <- function(df) {
  mat <- material_master()

  df <- df |>
    left_join(mat, by = c("Profit Ctr", "Product"))
  return(df)
}

join_product_hierarchy <- function(df) {
  ph <- master_ph()

  df <- df |>
    left_join(ph, by = c("Profit Ctr", "Product Hierarchy"))
  return(df)
}

process_copa_sales <- function(path) {
  df <- df |>
    join_customer_center() |>
    join_gl_accounts() |>
    join_with_poc() |>
    join_cm_cluster() |>
    join_customer_material() |>
    join_material_master() |>
    join_product_hierarchy()
  return(df)
}
