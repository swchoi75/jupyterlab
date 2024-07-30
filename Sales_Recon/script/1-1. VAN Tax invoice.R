library(readxl)
library(purrr)
library(dplyr)
library(stringr)
library(readr)


# Read and Get Excel sheet names ----
wb_path <- "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/VAN VT/202406/_202406 Tax invoice.xlsx"
wb_sheets <- excel_sheets(wb_path)
wb_sheets <- wb_sheets[1:23]          # Select sheets


# Concatenate worksheets into one data frame ----
df <- wb_sheets %>%
  set_names() %>%
  map_df(~ read_xlsx(path = wb_path, sheet = .x,
                     skip = 2, range = "A3:P2000", col_types = "text"))


# Remove unnecessary rows ----
df <- df %>%
  filter( ! `Customer PN` == 0 ) %>%
  filter( str_detect(`Customer PN`, "^[a-zA-Z0-9-_\\s]*$"))


# Convert types from text to double ----
df <- df %>%
  mutate(
    across(c(`입고수량`:`서열비`), as.double),
    across(c(`입고수량`:`서열비`), ~ tidyr::replace_na(.x, 0)),
    )


# Write UTF-8 CSV files with BOM ----
write_excel_csv(df, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/1-1. Tax invoice all.csv")
