library(dplyr)
library(readr)


# Group by & Summarize
group_summarize <- function(df) {
  df <- df |>
    group_by(pick(
      "Period", "Profit Ctr", "RecordType",
      "Cost Elem.", "Account Class",
      "Plnt", "Product", "MatNr Descr.", "Sold-to party", "Sold-to Name 1",
    )) |>
    summarise(
      Qty = sum(df$Quantity),
      Sales_LC = sum(df$TotSalesLC),
      STD_Costs = sum(df["Stock val."])
    ) |>
    ungroup()
  return(df)
}


# Filter out zero values
filter_out_zero <- function(df) {
  df <- df |>
    filter(!(
      df$Qty == 0 &
        df$Sales_LC == 0 &
        df$STD_Costs == 0
    ))
  return(df)
}



# Split Period into Year / Month
split_period <- function(df) {
  df <- df |>
    tidyr::separate(
      df$Period,
      into = c("Year", "Month"),
      sep = "\\."
    ) |>
    mutate(across(c("Year":"Month"), as.double))
  return(df)
}

# Rename columns
rename_columns <- function(df) {
  df <- df |>
    rename(c(`Material Description` = "MatNr Descr."))
  return(df)
}

wrangle_dataframe <- function(df) {
  df <- df |>

    group_summarize() |>
    filter_out_zero() |>
    split_period() |>
    rename_columns()
  return(df)
}

process_actual_data <- function(path) {
  df <- read_csv(path, show_col_types = FALSE) |>
    wrangle_dataframe()
  return(df)
}
