library(dplyr)
library(readxl)
library(purrr)
library(stringr)
library(readr)
library(here)
library(glue)


# Path
path <- here("Sales_Recon")


# Functions
read_excel_multiple_sheets <- function(file_path) {
  # select Excel sheets
  wb_sheets <- excel_sheets(file_path)
  wb_sheets <- wb_sheets[1:11]
  # read multiple excel sheets
  df <- wb_sheets |>
    set_names() |>
    map_df(
      ~ read_xlsx(
        path = file_path,
        sheet = .x,
        range = "A4:AI1000",
        col_types = "text"
      ),
      .id = "sheet"
    )
  return(df)
}


remove_unnecessary_row <- function(df) {
  df <- df |>
    filter(!.data$고객명 == 0) |>
    filter(!str_starts(.data$`Customer PN`, "A2C")) # Remove Kappa HEV ADJ
  return(df)
}


change_data_type <- function(df) {
  # Convert types from text to double
  df <- df |>
    mutate(
      across(c(15, 17, 19:35), as.double),
      across(c(15, 17, 19:35), ~ tidyr::replace_na(.x, 0)),
    )
  return(df)
}


format_price_list <- function(df) {
  # Format for Price List
  df <- df |>
    select(c(2:5, 7:8, 12:20)) |>
    # Remove MOBIS A/S for too much price change
    filter(!.data$고객명 == "MOBIS AS") |>
    filter(.data$Plant != 0) |>
    filter(.data$`Profit Center` != 0) |>
    select(!c(11:12)) |>
    arrange("Profit Center", "Customer PN rev")
  return(df)
}


format_price_diff <- function(df) {
  # Format for BU Price Difference
  df <- df |>
    select(c(4, 3, 7:8, 12, 14, 20:21, 23, 15:19, 26:27, 29, 31:34))
  return(df)
}


format_uninvoiced_qty <- function(df) {
  # Format for Uninvoiced
  df <- df |>
    select(c(2:4, 6, 12, 7:8, 13:14, 23, 19, 36)) |>
    filter(.data$조정Q != 0) |>
    mutate(
      `Order type` = case_when(
        조정Q > 0 ~ "ZOR",
        조정Q < 0 ~ "ZRE"
      ),
      `Order reason` = "C02",
      Adj_Qty = abs(.data$조정Q),
      이월체크 = "X",
    )
  return(df)
}


format_uninvoiced_amt <- function(df) {
  df <- df |>
    select(c(2:4, 6, 12, 7:8, 13:14, 29:36)) |>
    tidyr::pivot_longer(
      cols = c("조정금액":"sample"),
      names_to = "key",
      values_to = "value"
    ) |>
    filter(.data$value != 0) |>
    mutate(
      `Order type` = case_when(
        value > 0 ~ "ZDR",
        value < 0 ~ "ZCR"
      ),
      `Order reason` = case_when(
        key == "조정금액" ~ "A02",
        key == "단가소급" ~ "A03",
        key == "관세정산" ~ "A11",
        key == "환율차이" ~ "A04",
        key == "서열비" ~ "A12",
      ),
      `Adj_Amt` = abs(.data$value),
      `이월체크` = "",
    )
  return(df)
}


main <- function() {
  # Variables
  year <- "2024"
  month <- "06"

  # Filenames
  input_file <- here(
    path, "report in Excel",
    glue("{year}-{month} 입출고 비교.xlsx")
  )

  output_0 <- here(path, "output", "입출고비교 all.csv")
  output_1 <- here(path, "output", "입출고비교 to Price list.csv")
  output_2 <- here(path, "output", "입출고비교 to Price diff.csv")
  output_3 <- here(path, "output", "입출고비교 to Adj_Qty.csv")
  output_4 <- here(path, "output", "입출고비교 to Adj_Amt.csv")

  # Read data
  df <- read_excel_multiple_sheets(input_file)


  # Process data
  df <- df |>
    remove_unnecessary_row() |>
    change_data_type()

  price_list <- df |> format_price_list()
  price_diff <- df |> format_price_diff()
  adj_qty <- df |> format_uninvoiced_qty()
  adj_amt <- df |> format_uninvoiced_amt()

  # Write data
  write_excel_csv(df, output_0)
  write_excel_csv(price_list, output_1)
  write_excel_csv(price_diff, output_2)
  write_excel_csv(adj_qty, output_3)
  write_excel_csv(adj_amt, output_4)

  print("Files are created")
}

main()
