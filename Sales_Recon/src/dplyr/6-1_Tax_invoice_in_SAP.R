library(dplyr)
library(readr)
library(stringr)
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
  )

  # Process data
  df <- df |>
    select(c("X2", "X6", "X15")) |>
    rename(
      "Sold-to Party" = "X2",
      "Customer Name" = "X6",
      "Amount" = "X15",
    ) |>
    filter(!is.na(.data$`Sold-to Party`))

  df <- df |>
    mutate(
      across("Amount", ~ str_remove_all(.x, ",")),
      across("Amount", as.double)
    ) |>
    arrange("Sold-to Party")

  # Write data
  write_excel_csv(df, output_file, na = "0")
  print("A file is created")
}

main()