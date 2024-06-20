library(dplyr)
library(readr)
library(here)


# Path
path <- here("ZMPV")


# Functions
zmpv_ico <- function(df) {
  # Filter Intercompany
  df <- df %>%
    filter(.data[["outs_ic"]] == "IC")
  return(df)
}

zmpv_ppv <- function(df) {
  # Filter Net PPV
  df <- df %>%
    filter(.data[["net_pm_ppv"]] < -10000 | .data[["net_pm_ppv"]] > 10000)
  return(df)
}

zmpv_fx <- function(df) {
  # Filter FX Material (= former PPV FX)
  df <- df %>%
    filter(.data[["fx_effect"]] < -10000 | .data[["fx_effect"]] > 10000)
  return(df)
}

zmpv_loco <- function(df) {
  # Filter LOCO
  df <- df %>%
    filter(abs(.data[["std_tool_c"]]) + abs(.data[["std_freigh"]]) +
      abs(.data[["std_customs"]]) > 10000)
  return(df)
}

zmpv_smd <- function(df) {
  # Filter SMD Outsourcing
  df <- df %>%
    filter(
      .data[["vendor"]] == "0009085884" | # Geumhwa Electronics Co., Ltd.
        .data[["vendor"]] == "0009072686" # Nextech Co., Ltd
      #| `Vendor` == "0009149214"# Continental Automotive Electronics,
    ) %>%
    filter(.data[["gr_quantity"]] != 0)
  return(df)
}

main <- function() {
  db_path <- here(path, "db")
  output_path <- here(path, "output")

  # Filenames
  db_file <- here(db_path, "ZMPV_2024.csv")
  output_ico <- here(output_path, "ZMPV ICO purchase.csv")
  output_ppv <- here(output_path, "ZMPV Net PPV.csv")
  output_fx <- here(output_path, "ZMPV FX Material.csv")
  output_loco <- here(output_path, "ZMPV STD LOCO.csv")
  output_smd <- here(output_path, "ZMPV SMD outsourcing.csv")

  # Read data
  df <- read_csv(db_file, col_types = "ccc")

  # Process data
  df_ico <- zmpv_ico(df)
  df_ppv <- zmpv_ppv(df)
  df_fx <- zmpv_fx(df)
  df_loco <- zmpv_loco(df)
  df_smd <- zmpv_smd(df)

  # Write data
  write_csv(df_ico, output_ico, na = "0")
  write_csv(df_ppv, output_ppv, na = "0")
  write_csv(df_fx, output_fx, na = "0")
  write_csv(df_loco, output_loco, na = "0")
  write_csv(df_smd, output_smd, na = "0")
  print("Files are created")
}

main()
