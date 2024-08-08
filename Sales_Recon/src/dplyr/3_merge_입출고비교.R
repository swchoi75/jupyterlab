library(dplyr)
library(readxl)
library(purrr)
library(stringr)
library(readr)
library(janitor)
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
    filter(.data$고객명 != 0) |>
    filter(!str_starts(.data$customer_pn, "A2C")) # Remove Kappa HEV ADJ
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


format_price_list <- function(df, list_of_cols) {
  # Format for Price List
  df <- df |>
    select(all_of(list_of_cols)) |>
    # Remove MOBIS A/S for too much price change
    filter(.data$고객명 != "MOBIS AS") |>
    filter(.data$plant != 0) |>
    filter(.data$profit_center != 0) |>
    arrange(pick("profit_center", "customer_pn_rev"))
  return(df)
}


format_price_diff <- function(df, list_of_cols) {
  # Format for BU Price Difference
  df <- df |> select(all_of(list_of_cols))
  return(df)
}


format_uninvoiced_qty <- function(df, list_of_cols) {
  # Format for Uninvoiced
  df <- df |>
    select(all_of(list_of_cols)) |>
    filter(.data$조정_q != 0) |>
    mutate(
      order_type = case_when(
        조정_q > 0 ~ "ZOR",
        조정_q < 0 ~ "ZRE"
      ),
      order_reason = "C02",
      adj_qty = abs(.data$조정_q),
      이월체크 = "X",
    )
  return(df)
}


format_uninvoiced_amt <- function(df, list_of_cols) {
  df <- df |>
    select(all_of(list_of_cols)) |>
    tidyr::pivot_longer(
      cols = c("조정금액":"sample"),
      names_to = "key",
      values_to = "value"
    ) |>
    filter(.data$value != 0) |>
    mutate(
      order_type = case_when(
        value > 0 ~ "ZDR",
        value < 0 ~ "ZCR"
      ),
      order_reason = case_when(
        key == "조정금액" ~ "A02",
        key == "단가소급" ~ "A03",
        key == "관세정산" ~ "A11",
        key == "환율차이" ~ "A04",
        key == "서열비" ~ "A12",
      ),
      adj_amt = abs(.data$value),
      이월체크 = "",
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
  df <- read_excel_multiple_sheets(input_file) |>
    clean_names(ascii = FALSE)

  # Process data
  df <- df |>
    remove_unnecessary_row() |>
    change_data_type()

  columns_for_price_list <- c(
    "plant",
    "고객명",
    "sold_to",
    "공장명",
    "customer_pn_rev",
    "customer_pn",
    "mlfb",
    "div",
    "profit_center",
    "tax_invoice",
    "price_type",
    "sap_price",
    "입고_q"
  )
  price_list <- df |> format_price_list(columns_for_price_list)

  columns_for_price_diff <- c(
    "sold_to",
    "고객명",
    "customer_pn_rev",
    "customer_pn",
    "mlfb",
    "profit_center",
    "입고_q",
    "출고_q",
    "조정_q",
    "tax_invoice",
    "price_type_before_adj",
    "billing_price_before_adj",
    "price_type",
    "sap_price",
    "입고금액_jj_수량_단가",
    "출고금액",
    "조정금액",
    "단가소급",
    "관세정산",
    "환율차이",
    "서열비"
  )
  price_diff <- df |> format_price_diff(columns_for_price_diff)

  colummns_for_uninvoiced_qty <- c(
    "plant",
    "고객명",
    "sold_to",
    "ship_to",
    "mlfb",
    "customer_pn_rev",
    "customer_pn",
    "div",
    "profit_center",
    "조정_q",
    "sap_price",
    "comment"
  )
  adj_qty <- df |> format_uninvoiced_qty(colummns_for_uninvoiced_qty)

  colummns_for_uninvoiced_amt <- c(
    "plant",
    "고객명",
    "sold_to",
    "ship_to",
    "mlfb",
    "customer_pn_rev",
    "customer_pn",
    "div",
    "profit_center",
    "조정금액",
    "포장비",
    "단가소급",
    "관세정산",
    "환율차이",
    "서열비",
    "sample",
    "comment"
  )
  adj_amt <- df |> format_uninvoiced_amt(colummns_for_uninvoiced_amt)

  # Write data
  write_excel_csv(df, output_0)
  write_excel_csv(price_list, output_1)
  write_excel_csv(price_diff, output_2)
  write_excel_csv(adj_qty, output_3)
  write_excel_csv(adj_amt, output_4)

  print("Files are created")
}

main()
