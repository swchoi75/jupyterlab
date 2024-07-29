library(readxl)
library(purrr)
library(dplyr)
library(stringr)
library(readr)


# Read and Get Excel sheet names ----
wb_path <- "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/report in Excel/2024-06 입출고 비교.xlsx" # Monthly update
wb_sheets <- excel_sheets(wb_path)
wb_sheets <- wb_sheets[1:11] # Select sheets


# Concatenate worksheets into one data frame ----
df <- wb_sheets %>%
  set_names() %>%
  map_df(~ read_xlsx(
    path = wb_path, sheet = .x,
    range = "A4:AI1000", col_types = "text"
  ),
  .id = "sheet"
  )


# Remove unnecessary rows ----
df <- df %>%
  filter(!`고객명` == 0) %>%
  filter(!str_starts(`Customer PN`, "A2C")) # Remove Kappa HEV ADJ


# Convert types from text to double ----
df <- df %>%
  mutate(
    across(c(15, 17, 19:35), as.double),
    across(c(15, 17, 19:35), ~ tidyr::replace_na(.x, 0)),
  )


# Format for Price List ----
price_list <- df %>%
  select(c(2:5, 7:8, 12:20)) %>%
  filter(!`고객명` == "MOBIS AS") %>% # Remove MOBIS A/S for too much price change
  filter(!`Plant` == 0) %>%
  filter(!`Profit Center` == 0) %>%
  select(!c(11:12)) %>%
  arrange(`Profit Center`, `Customer PN rev`)


# Format for BU Price Difference ----
price_diff <- df %>%
  select(c(4, 3, 7:8, 12, 14, 20:21, 23, 15:19, 26:27, 29, 31:34))


# Format for Uninvoiced  ----
adj_qty <- df %>%
  select(c(2:4, 6, 12, 7:8, 13:14, 23, 19, 36)) %>%
  filter(!`조정Q` == 0) %>%
  mutate(
    `Order type` = case_when(
      `조정Q` > 0 ~ "ZOR",
      `조정Q` < 0 ~ "ZRE"
    ),
    `Order reason` = "C02",
    `Adj_Qty` = abs(`조정Q`),
    `이월체크` = "X",
  )

adj_amt <- df %>%
  select(c(2:4, 6, 12, 7:8, 13:14, 29:36)) %>%
  tidyr::pivot_longer(
    cols = c("조정금액":"sample"),
    names_to = "key",
    values_to = "value"
  ) %>%
  filter(!`value` == 0) %>%
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
    `Adj_Amt` = abs(value),
    `이월체크` = "",
  )


# Write UTF-8 CSV files with BOM ----
write_excel_csv(df, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/입출고비교 all.csv")
write_excel_csv(price_list, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/입출고비교 to Price list.csv", na = "")
write_excel_csv(price_diff, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/입출고비교 to Price diff.csv")
write_excel_csv(adj_qty, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/입출고비교 to Adj_Qty.csv")
write_excel_csv(adj_amt, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/입출고비교 to Adj_Amt.csv")
