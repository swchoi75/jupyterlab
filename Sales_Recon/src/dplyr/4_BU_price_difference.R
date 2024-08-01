library(dplyr)
library(readxl)
library(writexl)
library(readr)
library(purrr)
library(stringr)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Recon")


# Functions
read_txt_file <- function(file_path) {
  df <- read_tsv(file_path,
    skip = 6,
    locale = locale(encoding = "UTF-16LE"),
    col_types = list(.default = col_character()),
  )
  return(df)
}


read_excel_file <- function(file_path, sheet_number) {
  # select Excel sheet
  wb_sheets <- excel_sheets(file_path)
  wb_sheets <- wb_sheets[sheet_number]
  # read excel sheet
  df <- read_xlsx(path = file_path, sheet = wb_sheets)
  return(df)
}


add_material_master <- function(df, df_meta) {
  # Join main data with meta data ----
  df <- df |>
    left_join(df_meta, by = c(
      "mlfb" = "material",
      "profit_center" = "profit_center"
    ))
  return(df)
}


add_bu_outlet <- function(df) {
  # Add BU / Outlet Information
  df <- df |>
    mutate(
      outlet = case_when(
        profit_center == "50803-049" ~ "ENC",
        profit_center == "50803-051" ~ "MTC",
        profit_center == "50803-010" ~ "DTC",
        profit_center == "50803-026" ~ "HYD",
        profit_center == "50803-009" ~ "EAC",
        profit_center == "50803-063" ~ "DAC E",
        profit_center %in% c("50802-018", "50803-034") ~ "MES",
      ),
      .before = "sold_to"
    ) |>
    mutate(
      bu = case_when(
        outlet == "ENC" ~ "CT",
        outlet == "MTC" ~ "CT",
        outlet == "DTC" ~ "CT",
        outlet == "HYD" ~ "HT",
        outlet == "EAC" ~ "AC",
        outlet == "DAC E" ~ "AC",
        outlet == "MES" ~ "SC",
      ),
      .before = "outlet"
    ) |>
    arrange(.data$bu, .data$outlet)
  return(df)
}


add_blank_columns <- function(df) {
  # Add blank columns to fit to the Excel template ----
  df <- df |>
    mutate(a = "", .after = "outlet") |>
    mutate(b = "", .after = "출고_q") |>
    mutate(c = "", .after = "sap_price") |>
    mutate(d = "", e = "", f = "", .after = "조정금액") |>
    mutate(g = "", .after = "단가소급")
  return(df)
}


eliminate_sample <- function(df) {
  df <- df[df$mlfb != "sample", ]
  return(df)
}


main <- function() {
  # Variables
  source(here(path, "src", "dplyr", "common_variable.R"))

  # Filenames
  input_file <- here(path, "output", "입출고비교 to Price diff.csv")
  ## originally from project "Sales_Report"
  meta_1 <- here(path, "meta", glue("Material master_0180_{year}.xls"))
  meta_2 <- here(path, "meta", glue("Material master_2182_{year}.xls"))
  ## from project "Sales P3"
  meta_3 <- here(
    path, "../", "Sales_P3", "meta",
    glue("{year}_ACT & BUD MLFB mapping data.xlsx")
  )
  ## from current project
  meta_4 <- here(path, "meta", glue("Project Info with budget PN.xlsx"))

  output_file <- here(path, "output", "4. BU Price diff.csv")

  # Read data
  df <- read_csv(input_file, show_col_types = FALSE)
  mm_0180 <- read_txt_file(meta_1) |> clean_names(ascii = FALSE)
  mm_2182 <- read_txt_file(meta_2) |> clean_names(ascii = FALSE)
  df1 <- read_excel_file(meta_3, sheet_number = 2) |> clean_names(ascii = FALSE)
  df2 <- read_excel_file(meta_4, sheet_number = 1) |> clean_names(ascii = FALSE)

  # Process data
  mm <- bind_rows(mm_0180, mm_2182) |>
    rename("product_hierarchy" = "product_hierachy_11") |>
    select(c("material", "profit_center", "description"))

  df <- df |>
    add_material_master(mm) |>
    relocate("description", .after = "mlfb") |>
    select(!c("customer_pn_rev")) |>
    add_bu_outlet() |>
    add_blank_columns() |>
    eliminate_sample()

  df <- df |>
    left_join(df1, by = c("mlfb" = "material")) |>
    left_join(df2, by = c(bud_mlfb = "Material_local")) |>
    select(!c("bud_mlfb")) |>
    relocate("project_id", .after = "고객명") |>
    relocate("project_id_pivot", .after = "project_id")

  # Write data
  write_excel_csv(df, output_file, na = "")
  print("A file is created")
}

main()
