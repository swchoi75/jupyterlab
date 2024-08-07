library(dplyr)
library(readr)
library(stringr)
library(here)


# Path
path <- here("Sales_Recon")


# Functions
main <- function() {
  # Filenames
  input_1 <- here(path, "output", "1-1. Tax invoice all.csv")
  input_2 <- here(path, "output", "1-2. SAP billing summary.csv")
  output_file <- here(path, "output", "2-1. Join Customer PN.csv")

  # Read data
  df_1 <- read_csv(input_1, col_types = cols(.default = col_character()))
  df_2 <- read_csv(input_2, col_types = cols(.default = col_character()))

  # Process data
  selected_columns <- c("고객명", "sold_to_party", "customer_pn_rev")
  df_1 <- df_1 |> select(selected_columns)
  df_2 <- df_2 |> select(selected_columns)

  df <- full_join(df_1, df_2, by = selected_columns) |>
    distinct() |> # Unique values
    tidyr::drop_na() # Remove missing values

  # Write data
  write_excel_csv(df, output_file) # for 한글 표시
  print("A file is created")
}

main()
