library(readr)
library(dplyr)
library(stringr)


# Read a tab-delimited file ----
# Update monthly
df_0180 <- read_tsv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/SAP_results/Sales adjustment_0180_202406.xls",
  skip = 4,
  locale = locale(encoding = "UTF-16LE"),
  col_types = cols(.default = col_character()),
) %>%
  filter(!is.na(`Sales Organization`)) %>%
  select(-c(1, 2, 23))

# Update monthly
df_2182 <- read_tsv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/SAP_results/Sales adjustment_2182_202406.xls",
  skip = 4,
  locale = locale(encoding = "UTF-16LE"),
  col_types = cols(.default = col_character()),
) %>%
  filter(!is.na(`Sales Organization`)) %>%
  select(-c(1, 2, 23))


# Combine two dataframes ----
df_0180 <- df_0180 %>%
  mutate(Plant = "0180")

df_2182 <- df_2182 %>%
  mutate(Plant = "2182")

df <- bind_rows(df_0180, df_2182) %>%
  relocate(Plant, .after = `Sales Organization`)


# Two billing dates ----
df <- df %>%
  mutate(
    `Billing Day` = str_sub(`Billing Date`, 9, 10),
    .after = Plant
  )

# Filter out unneccessary rows on 2nd day ----
df <- df %>%
  filter(!(`Billing Day` == "02" &
    !str_detect(`Purchase Order`, "매출조정")
  ))


# Write UTF-8 CSV files with BOM ----
write_excel_csv(df, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/6-2. SAP uninvoiced sales.csv")
