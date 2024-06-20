library(dplyr)
library(readr)
library(janitor)


read_db <- function(path) {
  df <- read_csv(path, col_types = "ccc")
  return(df)
}

remove_unnecessary_columns <- function(df) {
  df <- df |>
    select(!c(
      "validity",
      "responsible",
      "account_description",
      "acc_lv1_by_consolidated",
      "acc_lv3",
      "acc_lv4",
      "acc_lv5",
      "acc_lv6",
    ))
}

process_numeric_columns <- function(df) {
  # Change sign logic, unit in k KRW, Add a new column
  df <- df |>
    mutate(across(
      c("actual", "plan", "target"),
      ~ (.x / -10^3)
    )) |>
    mutate(
      delta_to_plan = round(.data[["actual"]] - .data[["plan"]], 3),
      .after = "target"
    )
  return(df)
}

# Read master data ----
read_master_file <- function(filename) {
  read_csv(filename, show_col_types = FALSE) |> clean_names()
}

master_cc <- function(df_cc_general, df_cc_hierarchy) {
  df_cc_general |>
    left_join(df_cc_hierarchy, c("cctr" = "cctr"))
} # |> clean_names()

master_coom <- function(filename) {
  read_csv(filename, col_select = c(1:3), show_col_types = FALSE) |>
    clean_names()
}

# Operational costs ----
add_vol_diff <- function(df) {
  df <- df %>%
    mutate(
      volume_difference = round(.data[["plan"]] - .data[["target"]], 3),
      # .after = "delta_to_plan"
    )
  return(df)
}

process_master_data <- function(df, df_cc, df_acc, df_coom, df_poc) {
  df <- df %>%
    left_join(df_cc, c("costctr" = "cctr")) %>%
    left_join(df_acc, c("gl_accounts" = "account_no")) %>%
    left_join(df_coom, c(
      "costctr" = "cctr",
      "gl_accounts" = "account_no"
    )) %>%
    tidyr::separate(
      col = "lv3", into = c(NA, "function_2"),
      sep = "-", remove = FALSE
    ) %>%
    relocate("function_2", .after = last_col()) %>%
    left_join(df_poc, c("pctr" = "profit_center")) %>%
    rename(c(profit_center = "pctr")) %>%
    return(df)
}

# Process COOM data for fix and variable costs
split_fix_var <- function(df) {
  df <- df %>%
    mutate(coom = case_when(
      # handle budget topic
      fix_var == "Var" & gl_accounts == "K399" ~ "Var",
      is.na(coom) ~ "Fix",
      TRUE ~ coom,
    ))
  return(df)
}
