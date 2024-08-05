library(dplyr)
library(readr)
library(stringr)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Billing")


# Functions
process_ke30 <- function(df) {
  df <- df |>
    mutate(
      across(c("quantity", "revenues", "stock_val"), as.double)
    ) |>
    select(
      c(
        "period",
        "profit_center",
        "product",
        "quantity",
        "stock_val",
        "revenues"
      )
    ) |>
    mutate(
      period = str_sub(.data$period, 7, 8),
      source = "KE30", .before = "period"
    )
  return(df)
}


process_gl <- function(df) {
  df <- df |>
    mutate(
      across(
        c("quantity", "amount_in_doc_curr", "amount_in_local_cur"),
        as.double
      )
    ) |>
    select(c(
      "period",
      "profit_ctr",
      "material",
      "quantity",
      "amount_in_local_cur"
    )) |>
    rename(
      "profit_center" = "profit_ctr",
      "product" = "material",
      "stock_val" = "amount_in_local_cur"
    ) |>
    mutate(source = "GL Account", .before = "period")

  return(df)
}


summary_data <- function(df) {
  df <- df |>
    group_by(
      across(c("source", "period", "profit_center", "product")),
    ) |>
    summarise(
      quantity = sum(.data$quantity),
      sales = sum(.data$revenues),
      costs = sum(.data$stock_val),
      .groups = "drop"
    )
  return(df)
}


main <- function() {
  # Variables
  year <- "2024"

  # Filenames
  input_1 <- here(path, "db", glue("KE30_{year}.csv"))
  input_2 <- here(path, "db", glue("GL_{year}.csv"))
  output_file <- here(path, "output", glue("KE30 and GL Account_{year}.csv"))

  # Read data
  df_ke30 <- read_csv(input_1, col_types = list(.default = col_character()))
  df_gl <- read_csv(input_2, col_types = list(.default = col_character()))

  # Process data
  df_ke30 <- df_ke30 |> process_ke30()
  df_gl <- df_gl |> process_gl()
  df <- bind_rows(df_ke30, df_gl)

  df <- df |>
    summary_data() |>
    arrange(desc(source))

  # Write data
  write_csv(df, output_file)
  print("A file is created")
}

main()
