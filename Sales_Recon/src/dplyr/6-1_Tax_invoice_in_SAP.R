library(dplyr)
library(readr)
library(stringr)
library(janitor)
library(here)


# Path
path <- here("Sales_Recon")


# Functions
main <- function() {
  # Filenames
  input_file <- here(path, "data", "SAP", "Tax invoice by customer.xls")
  output_file <- here(path, "output", "6-1. Tax invoice in SAP.csv")


  # Read data
  df <- read_tsv(input_file,
    skip = 14,
    col_names = FALSE,
    locale = locale(encoding = "UTF-16LE"),
    col_types = cols(.default = col_character()),
  ) |> clean_names(ascii = FALSE)

  # Process data
  df <- df |>
    select(c("x2", "x6", "x15")) |>
    rename(
      "sold_to_party" = "x2",
      "customer_name" = "x6",
      "amount" = "x15",
    ) |>
    filter(!is.na(.data$sold_to_party))

  df <- df |>
    mutate(
      across("amount", ~ str_remove_all(.x, ",")),
      across("amount", as.double)
    ) |>
    arrange(.data$sold_to_party)

  # Write data
  write_excel_csv(df, output_file, na = "0")
  print("A file is created")
}

main()