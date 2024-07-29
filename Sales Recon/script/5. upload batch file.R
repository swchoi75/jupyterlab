library(readxl)
library(dplyr)
library(stringr)
library(writexl)
library(readr)


# Excel excel files ----
# Update monthly
df_qty <- read_xlsx("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/report in Excel/2024-06/2024-06 Sales adjustment batch_Qty.xlsx",
  sheet = "Format", col_types = "text"
)

df_amt <- read_xlsx("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/report in Excel/2024-06/2024-06 Sales adjustment batch_Price.xlsx",
  sheet = "Format", col_types = "text"
)


# Combine two dataframes & Remove unnecessary rows ----
df <- bind_rows(df_qty, df_amt) %>%
  filter(!Plant == "0") %>%
  filter(!`Order reason` == "NA") %>%
  mutate(across(c(Quantity, Price), as.double))



# Change 0 back to blank in 이월체크 ----
df <- df %>%
  mutate(
    `이월체크` = case_when(
      `이월체크` == "0" ~ "",
      TRUE ~ `이월체크`
    )
  )

# Data to be deleted ----
skip <- df %>%
  filter((`Order reason` == "C02" & Price == 0) | `Material` == "0")

df <- df %>%
  filter(!((`Order reason` == "C02" & Price == 0) | `Material` == "0"))


# Write UTF-8 CSV files with BOM ----
write_excel_csv(df, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/5. Upload batch file.csv", na = "")
write_excel_csv(skip, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/5. Upload to be skipped.csv", na = "")
