library(dplyr)
library(readr)
library(here)


# Path
path <- here("ZX05")
source(here(path, "src", "dplyr", "common_functions.R"))


# Functions
process_account <- function(df) {
  # Add account information
  df <- df |>
    mutate(
      ce_text = case_when(
        # E01-299
        acc_lv2 == "299 Total Labor Costs" ~ "10_Compensation",

        # E01-465
        acc_lv1 == "345 Depreciation long life" ~ "11_Depreciation & Leasing",
        acc_lv1 == "370 Rental/Leasing" ~ "11_Depreciation & Leasing",
        acc_lv1 == "375 Utilities" ~ "12_Energy",
        acc_lv1 == "435 Fees and purchased services"
        ~ "13_Fees and Purchased Services",
        acc_lv1 == "320 Purchased maintenance" ~ "15_Maintenance",
        acc_lv1 == "430 Entertainment/Travel expense" ~ "16_Travel Training",
        acc_lv1 == "440 Recruitment/Training/Development"
        ~ "16_Travel Training",
        acc_lv2 == "465 Cost of materials" ~ "17_Other cost",

        # E01-535
        gl_accounts == "K6620" ~ "18_Services In / Out",
        gl_accounts == "K6626" ~ "19_Transfer out",
        gl_accounts == "K6623" ~ "20_IT Allocation",
        gl_accounts == "K6624" ~ "20_IT Allocation",
        gl_accounts == "K6625" ~ "20_IT Allocation",

        # E01-520
        acc_lv2 == "520 Assessments In" ~ "CF cost assessment out",
      )
    )
  return(df)
}

main <- function() {
  # Filenames
  db_file <- here(path, "db", "CF_2024.csv")
  meta_acc <- here(path, "meta", "0000_TABLE_MASTER_Acc level.csv")
  meta_cc_general <- here(
    path, "meta",
    "0000_TABLE_MASTER_Cost center_general.csv"
  )
  meta_cc_hierarchy <- here(
    path, "meta",
    "0000_TABLE_MASTER_Cost center_hierarchy.csv"
  )
  output_file <- here(path, "output", "CF costs.csv")

  # Read data
  df <- read_db(db_file)
  df_acc <- read_master_file(meta_acc)
  df_cc_general <- read_master_file(meta_cc_general)
  df_cc_hierarchy <- read_master_file(meta_cc_hierarchy)
  df_cc <- master_cc(df_cc_general, df_cc_hierarchy)

  # Process data
  df <- df |>
    process_numeric_columns() |>
    left_join(df_cc, c("costctr" = "cctr")) |>
    left_join(df_acc, c("gl_accounts" = "account_no")) |>
    rename(c(profit_center = "pctr")) |>
    process_account() |>
    remove_unnecessary_columns()

  # Write data
  write_csv(df, output_file, na = "")
  print("A file is created")
}

main()
