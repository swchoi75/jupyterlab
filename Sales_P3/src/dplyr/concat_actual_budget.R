library(dplyr)
library(readr)
library(here)


# Path
path <- here("Sales_P3")


# Functions
main <- function() {
  # Filenames
  input_1 <- here(path, "output", "1_actual_sales.csv")
  input_2 <- here(path, "output", "3_budget_std_costs.csv")
  output_file <- here(path, "output", "4_actual_and_budget_sales.csv")

  # Read data
  df_act <- read_csv(input_1, show_col_types = FALSE)
  df_bud <- read_csv(input_2, show_col_types = FALSE)

  # Process data
  df <- bind_rows(
    df_act |> mutate(version = "Actual"),
    df_bud |> mutate(version = "Budget")
  )

  list_of_cols <- c(
    "version",
    "year",
    "month",
    # "period",
    "profit_ctr",
    "recordtype",
    "cost_elem",
    "account_class",
    "plnt",
    "product",
    "material_description",
    "sold_to_party",
    "sold_to_name_1",
    "qty",
    "sales_lc",
    "std_costs"
  )

  df <- df |> select(all_of(list_of_cols)) # reorder columns

  # Write data
  write_csv(df, output_file)
  print("A files is created")
}

main()
