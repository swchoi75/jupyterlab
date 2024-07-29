library(readr)
library(dplyr)
library(stringr)


# Read data files ----
df_1 <- read_csv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/1-1. Tax invoice all.csv",
  col_types = cols(.default = col_character()),
)
df_2 <- read_csv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/1-2. SAP billing summary.csv",
  col_types = cols(.default = col_character()),
)


# Select columns ----
df_1 <- df_1 %>%
  select(`고객명`, `Sold-to Party`, `Customer PN rev`)

df_2 <- df_2 %>%
  select(`고객명`, `Sold-to Party`, `Customer PN rev`)


# Join two dataframes ----
df <- df_1 %>%
  full_join(df_2, by = c("고객명", "Sold-to Party", "Customer PN rev"))


# Unique values ----
df <- df %>%
  distinct(`고객명`, `Sold-to Party`, `Customer PN rev`)


# Remove missing values ----
df <- df %>%
  filter(!is.na(`고객명`))


# Write UTF-8 CSV files with BOM ----
write_excel_csv(df, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/2-1. Join Customer PN.csv")
