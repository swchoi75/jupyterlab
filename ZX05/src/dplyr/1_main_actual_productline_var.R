library(dplyr)
library(readr)
library(stringr)
library(here)


# Path
path <- here("ZX05")
source(here(path, "src", "dplyr", "common_functions.R"))


# Functions
filter_var_account <- function(df) {
  # Var costs : Add account information
  df <- df |>
    filter(df$coom == "Var") |>
    # Remove S90xxx accounts
    filter(df$acc_lv6 != "Assessments to COPA") |>
    mutate(
      LDC_MDC = case_when(
        str_starts(costctr, "8") ~ "Start up costs",
        (function_2 == "FGK" & acc_lv2 == "299 Total Labor Costs") ~ "LDC",
        (function_2 == "FGK" & acc_lv2 == "465 Cost of materials") ~ "MDC"
      )
    ) |>
    mutate(
      ce_text = case_when(
        # LDC
        gl_accounts == "K250" ~ "120 Premium wages",
        gl_accounts == "K256" ~ "120 Premium wages",
        acc_lv1 == "158 Social benefit rate wages variable" ~ "158 SLB wages",
        acc_lv2 == "299 Total Labor Costs" ~ "115 Direct labor",
        # MDC
        TRUE ~ acc_lv1
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
  output_file <- here(path, "output", "PL var costs.csv")

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
    filter_var_account() |>
    remove_unnecessary_columns()

  # Write data
  write_csv(df, output_file, na = "")
  print("A file is created")
}

main()
