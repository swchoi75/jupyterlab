library(dplyr)
library(stringr)
library(readxl)
library(readr)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_P3")


# Functions
rename_columns <- function(df) {
  df <- df |>
    rename(c(
      "Product" = "Material (local)",
      "Profit Ctr" = "Profit Center",
      "Account Class" = "Ext./ICO",
      "Sold-to party" = "Sold-to (central)",
      "Sold-to Name 1" = "Sold-to (central) (pivot)"
    ))
  return(df)
}


select_columns <- function(df, year) {
  df <- df |>
    select(c(
      "Account Class",
      "Product",
      "Material Description",
      "Profit Ctr",
      "Sold-to party",
      "Sold-to Name 1",
      glue("{year}_01 vol"):glue("{year}_12 vol"),
      glue("{year}_01 k LC"):glue("{year}_12 k LC")
    ))
  return(df)
}


process_volume_and_amount <- function(df) {
  # Split data into Volume and Sales amount
  df_vol <- df |>
    select(c(1:"Sold-to Name 1", ends_with("vol")))

  df_amt <- df |>
    select(c(1:"Sold-to Name 1", ends_with("k LC")))

  # Tidyr::Pivot_longer
  df_vol <- df_vol |>
    tidyr::pivot_longer(
      cols = ends_with("vol"),
      names_to = "Period",
      values_to = "Qty",
    )

  df_amt <- df_amt |>
    tidyr::pivot_longer(
      cols = ends_with("k LC"),
      names_to = "Period",
      values_to = "Sales_k_LC",
    )

  # Combine data with Volume and Sales amount
  df <- bind_cols(
    df_vol, df_amt |> select("Sales_k_LC")
  ) |>
    mutate(
      Period = str_remove(.data$Period, " vol")
    )

  return(df)
}


process_miscellaneous <- function(df) {
  # To match with actual data, etc ----
  df <- df |>
    mutate(
      Sales_LC = df$Sales_k_LC * 1000,
      Plnt = case_when(
        df["Profit Ctr"] == "50802-018" ~ "2182",
        TRUE ~ "0180"
      )
    ) |>
    select(!c("Sales_k_LC")) |>
    select(c(
      "Period", "Profit Ctr", "Account Class",
      "Plnt", "Product", "Material Description",
      "Sold-to party", "Sold-to Name 1",
      "Qty", "Sales_LC"
    ))
  return(df)
}


split_period <- function(df) {
  # Split Period into Year / Month
  df <- df |>
    tidyr::separate(
      "Period",
      into = c("Year", "Month"),
      sep = "_"
    ) |>
    mutate(across(c("Year":"Month"), as.double))
  return(df)
}


arrange_by_month <- function(df) {
  df <- df |>
    arrange(.data$Month)
  return(df)
}


add_account_class <- function(df) {
  # Change Account Class to match with actual data
  df <- df |>
    mutate(`Account Class` = case_when(
      df["Account Class"] == "ICO" ~ "NSI",
      df["Account Class"] == "Ext." &
        str_detect(df["Sold-to Name 1"], "Continental") ~ "NSR",
      TRUE ~ "NSE"
    ))
  return(df)
}


add_record_type <- function(df) {
  # Add a new column RecordType
  df <- df |>
    mutate(
      RecordType = case_when(
        Product == "0" | is.na(Product) ~ "B",
        TRUE ~ "F"
      ),
      .after = "Profit Ctr"
    )
  return(df)
}

remove_na <- function(df) {
  # Remove NA values in Profit center
  df <- df |>
    filter(!is.na(df["Profit Ctr"]))
  return(df)
}


remove_zero <- function(df) {
  # Remove zero values in Qty and Sales
  df <- df |>
    filter(!(df$Qty == 0 & df$Sales_LC == 0))
  return(df)
}


main <- function() {
  # Variable
  year <- 2024

  # Filenames
  input_file <- here(
    path, "data", "plan",
    glue(
      "{year}_Consolidate_Sales_Budget 2024 CDP BIN_version_Incl.ICO_231004.xlsx"
    )
  )
  output_file <- here(path, "output", "2_budget_sales.csv")

  # Read data
  df <- read_excel(input_file, sheet = "Cons", skip = 2)

  # Process data
  df <- df |>
    rename_columns() |>
    select_columns(year) |>
    process_volume_and_amount() |>
    process_miscellaneous() |>
    split_period()

  df <- df |>
    arrange_by_month() |>
    add_account_class() |>
    add_record_type()

  df <- df |>
    remove_na() |>
    remove_zero() |>
    clean_names()

  # Write data
  write_csv(df, output_file)
  print("A file is created")
}

main()
