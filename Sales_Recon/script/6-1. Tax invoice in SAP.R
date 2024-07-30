library(readr)
library(dplyr)
library(stringr)


# Read a tab-delimited file ----
df <- read_tsv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/SAP/Tax invoice by customer.xls", # Update monthly
  skip = 14,
  col_names = FALSE,
  locale = locale(encoding = "UTF-16LE"),
  col_types = cols(.default = col_character()),
)

df <- df %>%
  select(c(X2, X6, X15)) %>%
  rename(
    `Sold-to Party` = X2,
    `Customer Name` = X6,
    `Amount` = X15,
  ) %>%
  filter(!is.na(`Sold-to Party`))

df <- df %>%
  mutate(
    across(Amount, ~ str_remove_all(.x, ",")),
    across(Amount, as.double)
  ) %>%
  arrange(`Sold-to Party`)


# Write UTF-8 CSV files with BOM ----
write_excel_csv(df, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/6-1. Tax invoice in SAP.csv", na = "0")
