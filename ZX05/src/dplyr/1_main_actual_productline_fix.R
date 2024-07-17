library(dplyr)
library(readr)
library(stringr)
library(here)


# Path
path <- here("ZX05")
source(here(path, "src", "dplyr", "common_functions.R"))


# Functions
filter_fix_account <- function(df) {
  # Fix costs : Add account information
  df <- df |>
    filter(df$coom == "Fix") |>
    # Remove S90xxx accounts
    filter(df$acc_lv6 != "Assessments to COPA") |>
    mutate(
      ce_text = case_when(
        # PV Costs : special logic for Division P in 2023 (temporary)
        (`function` == "PV" & gl_accounts == "S99116") ~ "12_PMME Others",
        # PV Costs
        `function` == "PV"
        ~ "10_Product Validation / Requalification after G60",
        str_sub(costctr, 1, 2) == "58"
        ~ "10_Product Validation / Requalification after G60",

        # E01-585
        gl_accounts == "K66270" ~ "01_NSHS Allocations in PE MGK & PE FGK",
        gl_accounts == "K66271" ~ "01_NSHS Allocations in PE MGK & PE FGK",
        gl_accounts == "K66280" ~ "02_NSHS Services in PE MGK & PE FGK",

        # E01-299
        acc_lv2 == "299 Total Labor Costs" ~ "06_Compensation",

        # E01-465
        gl_accounts == "K403"
        ~ "08_PMME Depreciation intangible development assets",
        acc_lv1 == "345 Depreciation long life"
        ~ "09_PMME Depreciation w/o intangible",
        acc_lv1 == "320 Purchased maintenance" ~ "07_Maintenance",
        acc_lv1 == "325 Project costs" ~ "11_Related project expenses (RPE)",

        # E01-520 / 525
        # FSC costs changed from PMME to FG&A from FY2023
        gl_accounts == "S87564" ~ "Assessment from FSC (CDP) to FG&A",
        # FF Assessment from FY2023 for QMPP reorganization
        gl_accounts == "S87310" ~ "04_Assessment from FF (520)",
        acc_lv2 == "520 Assessments In"
        ~ "03_Assessment from Central Functions (520)",
        # CM specific topic from 2024
        acc_lv2 == "525 Residual Costs"
        ~ "03_Assessment from Central Functions (520)",

        # E01-535
        # Check K6621 in budget
        gl_accounts == "K6626" ~ "05_Shared equipment \"K662x\" accounts",
        # K6620
        gl_accounts == "K6620" ~ "12_PMME Others_US regident Q engineer",

        # E01-630
        # S99xxx accounts for te-minute, tgb-minute, ast-hours

        TRUE ~ "12_PMME Others",
      )
    )
  return(df)
}

add_race_item <- function(df) {
  # Fix costs : Add RACE account information
  df <- df |>
    mutate(
      race_item = case_when(
        function_2 == "FGK" ~ "PE production",
        function_2 == "MGK" ~ "PE materials management",
        function_2 == "WVK" ~ "PE plant administration",
        function_2 == "VK" ~ "PE distribution",
        function_2 == "ALLOC" & gl_accounts == "S87564" ~ "F, G & A expenses",
        function_2 == "ALLOC" & gl_accounts == "K66271" ~ "PE production",
        function_2 == "ALLOC" & gl_accounts == "K66270"
        ~ "PE materials management",
        function_2 == "ALLOC" & gl_accounts == "K66273" ~ "PE selling",
        function_2 == "ALLOC" & gl_accounts == "K66275" ~ "PE communication",
        function_2 == "ALLOC" & gl_accounts == "K66278" ~ "F, G & A expenses",
        function_2 == "ALLOC" & gl_accounts == "K66281"
        ~ "R, D & E allocation in",
        function_2 == "ALLOC" & gl_accounts == "K66283"
        ~ "R, D & E allocation in",
      )
    )
  return(df)
}


main <- function() {
  # Filenames
  db_file <- here(path, "db", "PL_2024.csv")
  meta_acc <- here(path, "meta", "0000_TABLE_MASTER_Acc level.csv")
  meta_cc_general <- here(
    path, "meta",
    "0000_TABLE_MASTER_Cost center_general.csv"
  )
  meta_cc_hierarchy <- here(
    path, "meta",
    "0000_TABLE_MASTER_Cost center_hierarchy.csv"
  )
  meta_coom <- here(path, "meta", "0004_TABLE_MASTER_COOM_2024.csv")
  meta_poc <- here(path, "meta", "POC.csv")
  output_file <- here(path, "output", "PL fix costs.csv")

  # Read data
  df <- read_db(db_file)
  df_acc <- read_master_file(meta_acc)
  df_cc_general <- read_master_file(meta_cc_general)
  df_cc_hierarchy <- read_master_file(meta_cc_hierarchy)
  df_cc <- master_cc(df_cc_general, df_cc_hierarchy)
  df_coom <- master_coom(meta_coom)
  df_poc <- read_master_file(meta_poc)

  # Process data
  df <- df |>
    process_numeric_columns() |>
    add_vol_diff() |>
    process_master_data(df_cc, df_acc, df_coom, df_poc) |>
    split_fix_var() |>
    filter_fix_account() |>
    add_race_item() |>
    remove_unnecessary_columns()

  # Write data
  write_csv(df, output_file, na = "")
  print("A file is created")
}

main()
