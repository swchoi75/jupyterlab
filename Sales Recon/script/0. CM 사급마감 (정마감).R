library(readxl)
library(purrr)
library(dplyr)
library(readr)


# Read and Get Excel sheet names ----
wb_path <- "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/VAN CM/CAE VAN ALL 202406.xlsx"
wb_sheets <- excel_sheets(wb_path)
wb_sheets <- wb_sheets[2:7] # Select sheets


# Concatenate worksheets into one data frame ----
df <- wb_sheets %>%
  set_names() %>%
  map_df(~ read_xlsx(path = wb_path, sheet = .x, skip = 2, col_types = "text"), )


# Change types ----
df <- df %>%
  mutate(
    across(c(`입고누계수량`:`포장비누계`), as.double),
  )


# Filter out blank values ----
df <- df %>%
  filter(!`Part No` == 0)


# Read 사급업체 품번 Master ----
sub_1 <- read_xlsx("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/meta/사급업체 품번 Master.xlsx", range = "A2:J1000") %>%
  filter(!is.na(`업체`)) %>%
  distinct(`Customer P/N`, `Mat. Type`, `Div.`, `업체`)

sub_2 <- read_xlsx("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/meta/사급업체 품번 Master.xlsx", range = "A2:J1000") %>%
  filter(!is.na(`업체`)) %>%
  distinct(`CASCO Part No.`, `Mat. Type`, `Div.`, `업체`)


# Join two dataframes ----
df_1 <- df %>%
  left_join(sub_1, by = c("Part No" = "Customer P/N")) %>%
  filter(!is.na(`업체`)) %>%
  arrange(`업체`) %>%
  mutate(`구분` = "by Customer PN")

df_2 <- df %>%
  left_join(sub_2, by = c("CAE" = "CASCO Part No.")) %>%
  filter(!is.na(`업체`)) %>%
  arrange(`업체`) %>%
  mutate(`구분` = "by Material")


# Join two dataframes ----
df <- bind_rows(df_1, df_2)


# Write Excel files ----
write_excel_csv(df, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/VAN CM/result.csv")
