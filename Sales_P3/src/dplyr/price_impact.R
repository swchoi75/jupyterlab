library(dplyr)
library(tidyr)
library(readr)
library(here)


# Path
path <- here("Sales_P3")


# Functions
calculate_price_impact <- function(df) {
  df <- df |>
    mutate(
      act_price = ifelse(.data$version == "Budget" |
          .data$qty == 0, 0,
        round(.data$sales_lc / .data$qty, 0)
      ),
      price_diff = ifelse(.data$qty == 0, 0,
        .data$act_price - .data$bud_price
      ),
      price_impact = case_when(

        # Business logic
        .data$cm_cluster == "OES" ~ 0, # OES
        .data$product == "A2C1797520201" ~ 0, # Kappa HEV adjustment
        .data$product == "A2C1636530101" ~ 0, # Kappa HEV adjustment

        # General formula
        .data$qty == 0 ~ .data$sales_lc,
        is.na(.data$qty) ~ .data$sales_lc,
        TRUE ~ .data$qty * .data$price_diff
      ),
      price_impact_ratio = .data$price_impact / .data$sales_lc * 100,
    )
  return(df)
}


replace_missing_values <- function(df) {
  # Replace missing values with zero for numeric columns
  df <- df |>
    mutate(
      qty = replace_na(df$qty, 0),
      sales_lc = replace_na(df$sales_lc, 0),
      std_costs = replace_na(df$std_costs, 0),
      standard_price = replace_na(df$standard_price, 0),
      bud_price = replace_na(df$bud_price, 0),
      act_price = replace_na(df$act_price, 0),
      price_diff = replace_na(df$price_diff, 0),
      price_impact = replace_na(df$price_impact, 0),
      price_impact_ratio = replace_na(df$price_impact_ratio, 0),
    )
  return(df)
}


main <- function() {
  # Filenames
  input_file <- here(path, "output", "6_ytd_sales_spv_mapping.csv")
  output_file <- here(path, "output", "7_ytd_sales_price_impact.csv")

  # Read data
  df <- read_csv(input_file, show_col_types = FALSE)

  # Process data
  df <- df |>
    calculate_price_impact() |>
    replace_missing_values()

  # Write data
  write_csv(df, output_file)

  print("A file is created")
}

main()