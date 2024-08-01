library(dplyr)
library(readr)
library(stringr)
library(here)


# Path
path <- here("Sales_Recon")


# Functions
summary_data <- function(df) {
  # Summary of data ----
  df <- df |>
    group_by(
      pick("고객명", "sold_to_party", "customer_pn_rev")
    ) |>
    summarise(
      입고Q = sum(.data$입고수량),
      입고금액 = sum(.data$입고금액),
      포장비 = sum(.data$포장비),
      단가소급 = sum(.data$단가소급),
      관세정산 = sum(.data$관세정산),
      sample = sum(.data$sample),
      # to remove 0 or NA values
      glovis_price = mean(.data$glovis_price[.data$glovis_price > 0],
        na.rm = TRUE
      ),
      서열비 = sum(.data$서열비),
    )
  return(df)
}


summary_df2 <- function(df) {
  df <- df |>
    group_by(
      pick("고객명", "sold_to_party", "customer_pn_rev")
    ) |>
    summarise(
      출고Q = sum(.data$qty),
      출고금액 = sum(.data$amt),
    )
  return(df)
}


main <- function() {
  # Filenames
  input_0 <- here(path, "output", "2-1. Join Customer PN.csv")
  input_1 <- here(path, "output", "1-1. Tax invoice all.csv")
  input_2 <- here(path, "output", "1-2. SAP billing summary.csv")
  output_file <- here(
    path, "output",
    "2-2. Join Tax invoice and SAP billing.csv"
  )

  # Read data
  df <- read_csv(input_0, col_types = cols(.default = col_character()))
  df_1 <- read_csv(input_1, col_types = cols(.default = col_character()))
  df_2 <- read_csv(input_2, col_types = cols(.default = col_character()))

  # Process data
  df_1 <- df_1 |>
    mutate(across(c("입고수량":"서열비"), as.double)) |>
    select(c("고객명", "sold_to_party", "customer_pn_rev", "입고수량":"서열비"))

  df_2 <- df_2 |>
    mutate(
      across(c("current_price", "qty":"avg_billing_price"), as.double),
    ) |>
    select(c("고객명", "sold_to_party", "customer_pn_rev", "qty", "amt")) |>
    filter(!is.na(.data$고객명)) |>
    summary_df2()

  df <- df |>
    left_join(df_1, by = c("고객명", "sold_to_party", "customer_pn_rev")) |>
    summary_data() |>
    left_join(df_2, by = c("고객명", "sold_to_party", "customer_pn_rev"))

  # Write data
  write_excel_csv(df, output_file, na = "0")
  print("A file is created")
}

main()
