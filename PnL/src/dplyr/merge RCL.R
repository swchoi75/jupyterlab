library(dplyr)
library(readr)
library(purrr)
library(stringr)
library(readxl)
library(fs)
library(janitor)
library(here)


# Path
path <- here("PnL")


# Functions
read_multiple_files <- function(list_of_files) {
  df <- list_of_files |>
    map_dfr(read_excel,
      sheet = "RCL Input GC", skip = 4, .id = "source",
      col_types = c(
        "text", "numeric", "numeric", "numeric",
        "text", "numeric", "numeric", "numeric",
        "text", "numeric", "numeric", "numeric",
        "text", "numeric", "numeric", "numeric",
        "text", "numeric", "numeric", "numeric",
        "numeric", "numeric", "numeric"
      ),
    )
  return(df)
}

rename_columns <- function(df) {
  df <- df |>
    select(!c("ACT Period GC":"FC GC fx impact")) |>
    rename(c(
      `Period Plan`         = "Single Period BUD GC",
      `Period Act`          = "Single Period ACT GC @ BUD fx",
      `Delta Period`        = "Single Period ACT-BUD GC @ BUD fx",
      `Comments Period`     = "Comment Single Period ACT-BUD Period",
      `YTD Plan`            = "YTD BUD GC",
      `YTD Act`             = "YTD ACT GC @ BUD fx",
      `Delta YTD`           = "YTD ACT-BUD GC @ BUD fx",
      `Comments YTD`        = "Comment YTD ACT-BUD",
      `Plan`                = "FY BUD GC",
      `FC`                  = "FC GC @ BUD fx...11",
      `Delta to Plan`       = "FC - BUD GC @ BUD fx",
      `Comments FC`         = "Comment FC - BUD GC",
      `LFC`                 = "LFC GC @ BUD fx",
      `FC_`                 = "FC GC @ BUD fx...15",
      `Delta to LFC`        = "FC - LFC GC @ BUD fx",
      `Comments FC changes` = "Comment FC - LFC"
    ))
  return(df)
}

convert_currency <- function(df, bud_fx) {
  # Change GC to LC
  df <- df |>
    mutate(across(where(is.numeric), ~ .x * bud_fx))
  return(df)
}

extract_poc <- function(df) {
  # Get Outlet, Plant infomration from file names using Regex
  df <- df |>
    mutate(
      outlet_plant = str_extract(source, "[0-9\\_]{7,9}"),
      outlet = str_extract(df$outlet_plant, "[0-9]{3,4}"),
      plant = str_extract(df$outlet_plant, "[0-9]{3}$")
    ) |>
    select(!c("source", "outlet_plant")) |>
    relocate(c("outlet", "plant"))
  return(df)
}

# Plant Outlet Combination
poc <- function(filename) {
  df <- read_csv(filename, col_types = "ccc") |> # "c" as Character type
    clean_names() |>
    select(!c("cu", "profit_center"))
  return(df)
}

join_with_poc <- function(df, filename) {
  df <- df |>
    left_join(poc(filename), by = c("outlet", "plant")) |>
    relocate(c("division", "bu", "outlet_name", "plant_name"))
  return(df)
}

# Add key column for look up RCL comments
add_key_column <- function(df) {
  df <- df |>
    tidyr::unite("key", c("outlet", "plant", "rcl_item_structure"),
      remove = FALSE
    ) |>
    relocate("key", .after = last_col())
  return(df)
}

main <- function() {
  # Variables
  bud_fx <- 1329 # Budget FX rate in 2023

  # Path
  data_path <- here(path, "data", "RCL")

  # Filenames
  xls_files <- dir_ls(data_path, regexp = "\\.xlsm$")
  meta_file <- here(path, "meta", "POC.csv")
  output_file <- here(path, "output", "RCL.csv")


  # Read data
  df <- read_multiple_files(xls_files)

  # Process data
  df <- df |>
    rename_columns() |>
    clean_names() |>
    convert_currency(bud_fx) |>
    extract_poc() |>
    join_with_poc(meta_file) |>
    add_key_column()


  # Write data
  write_csv(df, output_file, na = "")
}

main()
