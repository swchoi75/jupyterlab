library(dplyr)
source("script/master_data.R")


missing_customer_center <- function(df) {
  cc <- master_cc()

  df <- df |>
    distinct(df["Sold-to party"], df["Sold-to Name 1"]) |>
    tidyr::drop_na(df["Sold-to party"]) |>
    anti_join(cc, by = "Sold-to party")
  return(df)
}

missing_gl_accounts <- function(df) {
  gl <- master_gl() |>
    select(c("G/L account", "G/L account name"))

  df <- df |>
    distinct(df["Cost Elem."]) |>
    tidyr::drop_na(df["Cost Elem."]) |>
    anti_join(gl, by = c("Cost Elem." = "G/L account"))
  return(df)
}

missing_material_master <- function(df) {
  mat <- material_master()

  df <- df |>
    distinct(df["Profit Ctr"], df$Product) |>
    tidyr::drop_na(df$Product) |>
    anti_join(mat, by = c("Profit Ctr", "Product"))
  return(df)
}

missing_customer_material <- function(df) {
  df <- df |>
    filter(df$Division == "E") |>
    filter(is.na(df["Customer Material"])) |>
    filter(df["Material type"] %in% c("FERT", "HALB", "HAWA")) |>
    select(c(
      "Version",
      "Month",
      "Profit Ctr",
      "Product",
      "Material Description"
    ))
  return(df)
}

# filter missing Product hierarchy except CM profit centers
missing_product_hierarchy <- function(df) {
  ph <- master_ph()
  cm_profitctr <- c("50803-045", "50803-044", "50803-046")

  df <- df |>
    filter(!df["Profit Ctr"] %in% cm_profitctr) |>
    distinct(df["Profit Ctr"], df["Product Hierarchy"]) |>
    tidyr::drop_na(df["Product Hierarchy"]) |>
    anti_join(ph, by = c("Profit Ctr", "Product Hierarchy"))
  return(df)
}
