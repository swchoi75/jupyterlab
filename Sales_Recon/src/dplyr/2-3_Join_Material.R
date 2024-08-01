library(dplyr)
library(readr)
library(stringr)
library(here)


# Path
path <- here("Sales_Recon")


# Functions
main <- function() {
  # Filenames
  input_0 <- here(path, "output", "2-2. Join Tax invoice and SAP billing.csv")
  input_1 <- here(path, "output", "1-1. Tax invoice all.csv")
  input_2 <- here(path, "output", "1-2. SAP billing summary.csv")
  output_file <- here(path, "output", "2-3. 입출고 비교.csv")

  # Read data
  df <- read_csv(input_0, col_types = cols(.default = col_character()))
  df_1 <- read_csv(input_1, col_types = cols(.default = col_character()))
  df_2 <- read_csv(input_2, col_types = cols(.default = col_character()))

  # Process data
  # Select columns and Remove duplicate rows ----
  df_1 <- df_1 |>
    select(c("고객명":"Customer PN rev")) |>
    tidyr::unite("temp", c("고객명", "Sold-to Party", "Customer PN rev"),
      sep = "_", remove = FALSE
    )

  df_2 <- df_2 |>
    filter(!is.na(.data$고객명)) |>
    select(c("Plant":"Curr")) |>
    tidyr::unite("temp", c("고객명", "Sold-to Party", "Customer PN rev"),
      sep = "_", remove = FALSE
    )

  # Remove duplicate rows ----
  df_1a <- df_1 |>
    filter(!duplicated(.data$temp)) |>
    select(!c("temp"))

  df_2a <- df_2 |>
    filter(!duplicated(.data$temp)) |>
    select(!c("temp"))

  df_2b <- df_2 |>
    filter(duplicated(.data$temp)) |>
    select(!c("temp"))


  # Join two dataframes ----
  df <- df |>
    left_join(df_2a, by = c("고객명", "Sold-to Party", "Customer PN rev"))

  df <- df |>
    left_join(df_1a, by = c("고객명", "Sold-to Party", "Customer PN rev"))


  # bind_rows ----
  df <- df |>
    bind_rows(df_2b)


  # Arrange rows by column values ----
  df <- df |>
    arrange(
      .data$고객명,
      .data$`Sold-to Party`,
      .data$`Customer PN rev`,
      .data$Plant,
      .data$`Profit Center`,
      .data$`Material Number`
    )

  # Write data
  write_excel_csv(df, output_file, na = "0")
  print("A file is created")
}

main()
