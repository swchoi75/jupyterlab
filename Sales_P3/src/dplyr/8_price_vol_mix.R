library(dplyr)
library(readr)
library(janitor)
library(here)


# Path
path <- here("Sales_P3")


# Functions
add_cm_ratio <- function(df, df_meta) {
  df <- df |>
    left_join(df_meta, by = "profit_ctr")
  return(df)
}


add_delta_sales <- function(df) {
  df <- df |>
    mutate(
      delta_sales = ifelse(.data$version == "Actual",
        .data$sales_lc,
        .data$sales_lc * -1
      )
    )
  return(df)
}


add_delta_sales_price <- function(df) {
  df <- df |>
    mutate(
      delta_sales_price = ifelse(.data$version == "Actual",
        .data$price_impact,
        .data$price_impact * -1
      )
    )
  return(df)
}


add_delta_sales_vol <- function(df) {
  df <- df |>
    mutate(
      delta_sales_volume = .data$delta_sales - .data$delta_sales_price
    )
  return(df)
}


add_delta_margin <- function(df) {
  df <- df |>
    mutate(
      delta_margin = ifelse(.data$version == "Actual",
        .data$sales_lc - .data$std_costs,
        (.data$sales_lc - .data$std_costs) * -1
      )
    )
  return(df)
}


add_delta_margin_price <- function(df) {
  df <- df |>
    mutate(
      delta_margin_price = .data$delta_sales_price
    )
  return(df)
}


add_delta_margin_vol <- function(df) {
  df <- df |>
    mutate(
      delta_margin_volume =
        .data$delta_sales_volume * .data$cm_ratio / 100
    )
  return(df)
}


add_delta_margin_mix <- function(df) {
  df <- df |>
    mutate(
      delta_margin_mix = .data$delta_margin -
        .data$delta_margin_price - .data$delta_margin_volume
    )
  return(df)
}


main <- function() {
  # Filenames
  input_file <- here(path, "output", "7_ytd_sales_price_impact.csv")
  meta_file <- here(path, "meta", "Budget Contribution Margin ratio.csv")
  output_file <- here(path, "output", "ytd_sales_p3_impact.csv")

  # Read data
  df <- read_csv(input_file, show_col_types = FALSE)
  df_meta <- read_csv(meta_file, show_col_types = FALSE) |>
    select(c("Profit Ctr", "CM ratio")) |>
    clean_names()

  # Process data
  df <- df |>
    add_cm_ratio(df_meta) |>
    add_delta_sales() |>
    add_delta_sales_price() |>
    add_delta_sales_vol() |>
    add_delta_margin() |>
    add_delta_margin_price() |>
    add_delta_margin_vol() |>
    add_delta_margin_mix()

  # Write data
  write_csv(df, output_file)

  print("A file is created")
}

main()
