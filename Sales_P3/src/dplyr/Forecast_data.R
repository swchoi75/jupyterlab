library(dplyr)
library(stringr)
library(readxl)


# Read an Excel sheet
read_excel_file <- function(path) {
  df <- read_excel(path, sheet = "Sheet1", skip = 2)
  return(df)
}

# Select & rename columns
select_columns <- function(df) {
  df <- df |>
    select(c(
      "Ext./ICO",
      "Material (local)", "Material Description", "Profit Center",
      "Sold-to (central)", "Sold-to (central) (pivot)",
      "2023_01 vol":"2023_12 vol",
      "2023_01 k LC":"2023_12 k LC"
    ))
  return(df)
}

rename_columns <- function(df) {
  df <- df |>
    rename(c(
      Product = "Material (local)",
      `Profit Ctr` = "Profit Center",
      `Account Class` = "Ext./ICO",
      `Sold-to party` = "Sold-to (central)",
      `Sold-to Name 1` = "Sold-to (central) (pivot)"
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
      Period = str_remove(df$Period, " vol")
    )

  return(df)
}

# To match with actual data, etc ----
process_miscellaneous <- function(df) {
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

# Split Period into Year / Month
split_period <- function(df) {
  df <- df |>
    tidyr::separate(
      df$Period,
      into = c("Year", "Month"),
      sep = "_"
    ) |>
    mutate(across(c("Year":"Month"), as.double)) |>
    # add 1 to match with SAP actual data
    mutate(Year = df$Year + 1)
  return(df)
}

arrange_by_month <- function(df) {
  df <- df |>
    arrange(.data$Month)
  return(df)
}

# Change Account Class to match with actual data
account_class <- function(df) {
  df <- df |>
    mutate(`Account Class` = case_when(
      df["Account Class"] == "ICO" ~ "NSI",
      df["Account Class"] == "Ext." &
        str_detect(df["Sold-to Name 1"], "Continental") ~ "NSR",
      TRUE ~ "NSE"
    ))
  return(df)
}

# Add a new column RecordType
record_type <- function(df) {
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

# Remove NA values in Profit center
remove_na <- function(df) {
  df <- df |>
    filter(!is.na(df["Profit Ctr"]))
  return(df)
}

# Remove zero values in Qty and Sales
remove_zero <- function(df) {
  df <- df |>
    filter(!(df$Qty == 0 & df$Sales_LC == 0))
  return(df)
}

wrangle_dataframe <- function(df) {
  df <- df |>
    select_columns() |>
    rename_columns() |>
    process_volume_and_amount() |>
    process_miscellaneous() |>
    split_period() |>
    arrange_by_month() |>
    account_class() |>
    record_type() |>
    remove_na() |>
    remove_zero()
  return(df)
}

process_forecast_data <- function(path) {
  df <- read_excel(path, sheet = "Sheet1", skip = 2) |>
    wrangle_dataframe()
  return(df)
}
