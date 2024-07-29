library(readr)
library(dplyr)
library(stringr)
library(readxl)


# Read Excel files ----
df_0180 <- read_excel("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/SAP/Billing_0180_2024_06.xlsx") %>% # Update monthly
filter(!is.na(`Sales Organization`))


df_2182 <- read_excel("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/SAP/Billing_2182_2024_06.xlsx") %>% # Update monthly
filter(!is.na(`Sales Organization`))


# Combine files ----
df <- df_0180 %>%
  bind_rows(df_2182)


# Select columns ----
df1 <- df %>%
  select(`Material Number`)

output <- df1 %>%
  distinct(`Material Number`)

  
# Write UTF-8 CSV files with BOM ----
write_tsv(output, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/1-2-1. list of price download.xls", na = "")
