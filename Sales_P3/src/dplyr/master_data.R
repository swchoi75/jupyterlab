library(dplyr)
library(readr)
library(readxl)


# Master data ----
master_cc <- function() {
  df <- read_csv("meta/CC_2023.csv", show_col_types = FALSE)
  return(df)
}

master_gl <- function() {
  df <- read_csv("meta/GL.csv", show_col_types = FALSE)
  return(df)
}

master_ph <- function() {
  df <- read_csv("meta/PH info.csv", show_col_types = FALSE) |>
    rename(c(
      `Profit Ctr` = "Profit Center"
    )) |>
    select(c(
      "Profit Ctr",
      "Product Hierarchy",
      "PH_3 simple",
      "PRD/MER"
    ))
  return(df)
}

master_poc <- function() {
  df <- read_csv("../PnL/meta/POC.csv", show_col_types = FALSE)
  return(df)
}

master_cm_cluster <- function() {
  df <- read_excel("meta/YPC1 costing_Icheon.xlsx")
  return(df)
}

customer_material <- function() {
  df <- read_excel("meta/Customer Material.xlsx", sheet = "Sheet1") |>
    select(c(
      "Product",
      "Customer Material"
    )) |>
    filter(df["Customer Material"] != "NA")
  return(df)
}


# Material master ----
mat_0180 <- "meta/Material master_0180.xlsx"
mat_2182 <- "meta/Material master_2182.xlsx"

master_material <- function(path) {
  df <- read_excel(path, sheet = "MM")

  df <- df |>
    rename(c(
      `Product Hierarchy` = "Product Hierachy...8"
    )) |>
    select(c(
      "Material",
      "Profit Center",
      "Material type",
      "Product Hierarchy",
      "Standard Price",
      "Ext. Matl. Group"
    )) |>
    rename(c(
      Product = "Material",
      `Profit Ctr` = "Profit Center",
    ))

  return(df)
}

master_mat_0180 <- function() {
  df <- master_material(mat_0180)
  return(df)
}

master_mat_2182 <- function() {
  df <- master_material(mat_2182)
  return(df)
}

material_master <- function() {
  df <- bind_rows(master_mat_0180(), master_mat_2182())
  return(df)
}
