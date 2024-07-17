library(dplyr)
library(readr)
library(here)


# Path
path <- here("ZVAR")


# Functions
zvar_act <- function(df, cols_common, cols_activity) {
  # Select activity variance
  df <- df |>
    select(all_of(cols_common), all_of(cols_activity)) |>
    filter(!is.na(df$acttyp)) |>
    filter(is.na(df$no_post))
  return(df)
}

zvar_mat_full <- function(df, cols_common, cols_material) {
  # Select material usage variance
  df <- df |>
    select(all_of(cols_common), all_of(cols_material)) |>
    filter(is.na(df$acttyp)) |>
    filter(is.na(df$no_post)) |>
    # Filter out zero values
    filter(!(df$mvqn == 0 & df$mvsu == 0 &
               df$mvpr == 0 & df$bset == 0))
  return(df)
}

zvar_mat_major <- function(df) {
  # Select material usage variance
  df <- df |>
    filter(
      abs(
        df$mvqn + df$mvsu + df$mvpr + df$bset
      ) > 10^5
    )
  return(df)
}

zvar_oth <- function(df, cols_common, cols_others) {
  # Select other variance
  df <- df |>
    select(all_of(cols_common), all_of(cols_others)) |>
    filter(is.na(df$acttyp)) |>
    filter(!is.na(df$no_post))
  return(df)
}

main <- function() {
  # Variables
  cols_common <- c(
    "order",
    "material",
    "cost_elem_",
    "comaterial",
    "cost_ctr",
    "acttyp",
    "d_c",
    "um",
    "act_costs",
    "targ_costs",
    "var_amount",
    "actual_qty",
    "target_qty",
    "qty_varian",
    "product_hierarchy",
    "blk_ind",
    "no_post",
    "per", # "per" is period
    "year",
    "itm",
    "type",
    "cat",
    "prctr_cc",
    "description",
    "profit_ctr"
  )

  cols_activity <- c("avsu", "avqn", "avpr")
  cols_material <- c("mvsu", "mvqn", "mvpr", "bset") # "bset" is bulk variance
  cols_others <- c("pvar") # "pvar" is total variance

  # Filenames
  db_file <- here(path, "db", "ZVAR_2024.csv")
  output_1 <- here(path, "output", "ZVAR Activity variance.csv")
  output_2 <- here(path, "output", "ZVAR Material usage variance_full.csv")
  output_3 <- here(path, "output", "ZVAR Material usage variance_major.csv")
  output_4 <- here(path, "output", "ZVAR no post others.csv")

  # Read a data file in CSV format
  df <- read_csv(db_file, show_col_types = FALSE)

  # Process data
  df_act <- zvar_act(df, cols_common, cols_activity)
  df_mat_full <- zvar_mat_full(df, cols_common, cols_material)
  df_mat_major <- zvar_mat_major(df_mat_full)
  df_oth <- zvar_oth(df, cols_common, cols_others)

  # Write data
  write_csv(df_act, output_1, na = "0")
  write_csv(df_mat_full, output_2, na = "0")
  write_csv(df_mat_major, output_3, na = "0")
  write_csv(df_oth, output_4, na = "0")
  print("Files are created")
}

main()
