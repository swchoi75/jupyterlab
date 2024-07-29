library(dplyr)
library(readxl)
library(writexl)
library(readr)
library(purrr)
library(stringr)


# Read CSV files ----
df <- read_csv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/입출고비교 to Price diff.csv", show_col_types = FALSE)


# Read meta data in tab-delimited files ----
mm_0180 <- read_tsv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Report/meta/Material master_0180_2024.xls",
  skip = 6,
  locale = locale(encoding = "UTF-16LE"),
  col_types = list(.default = col_character()),
)

mm_2182 <- read_tsv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Report/meta/Material master_2182_2024.xls",
  skip = 6,
  locale = locale(encoding = "UTF-16LE"),
  col_types = list(.default = col_character()),
)

mm <- bind_rows(mm_0180, mm_2182) %>%
  rename(`Product Hierarchy` = `Product Hierachy...11`) %>%
  select(`Material`, `Profit Center`, `Description`)



wb_path <- "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales P3/meta/2024_ACT & BUD MLFB mapping data.xlsx"
wb_sheets <- excel_sheets(wb_path)
wb_sheets <- wb_sheets[[2]]

df1 <- read_xlsx(path = wb_path, sheet = wb_sheets)


wb_path <- "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/meta/Project Info with budget PN.xlsx"
wb_sheets <- excel_sheets(wb_path)
wb_sheets <- wb_sheets[[1]]

df2 <- read_xlsx(path = wb_path, sheet = wb_sheets)


# Join main data with meta data ----
df <- df %>%
  left_join(mm, by = c(
    "MLFB" = "Material",
    "Profit Center" = "Profit Center"
  ))


# Miscellaneous ----
df <- df %>%
  relocate(Description, .after = MLFB) %>%
  select(-c(`Customer PN rev`))


# Add BU / Outlet Information ----
df <- df %>%
  mutate(
    outlet = case_when(
      `Profit Center` == "50803-049" ~ "ENC",
      `Profit Center` == "50803-051" ~ "MTC",
      `Profit Center` == "50803-010" ~ "DTC",
      `Profit Center` == "50803-026" ~ "HYD",
      `Profit Center` == "50803-009" ~ "EAC",
      `Profit Center` == "50803-063" ~ "DAC E",
      `Profit Center` %in% c("50802-018", "50803-034") ~ "MES",
    ),
    .before = `sold to`
  ) %>%
  mutate(
    BU = case_when(
      outlet == "ENC" ~ "CT",
      outlet == "MTC" ~ "CT",
      outlet == "DTC" ~ "CT",
      outlet == "HYD" ~ "HT",
      outlet == "EAC" ~ "AC",
      outlet == "DAC E" ~ "AC",
      outlet == "MES" ~ "SC",
    ),
    .before = outlet
  ) %>%
  arrange(BU, outlet)


# Add blank columns to fit to the Excel template ----
df <- df %>%
  mutate(a = "", .after = outlet) %>%
  mutate(b = "", .after = `출고Q`) %>%
  mutate(c = "", .after = `SAP Price`) %>%
  mutate(d = "", e = "", f = "", .after = "조정금액") %>%
  mutate(g = "", .after = `단가소급`)


# Samples elimination ----
df <- df[df$MLFB != "Sample", ]


df <- df %>%
  left_join(df1, by = c("MLFB" = "Material"))


df <- df %>%
  left_join(df2, by = c(`BUD MLFB` = "Material(local)"))


df <- df %>%
  select(-`BUD MLFB`)


df <- df %>%
  relocate(`Project ID`, .after = 고객명)

df <- df %>%
  relocate(`Project ID (pivot)`, .after = `Project ID`)


# Write a CSV file ----
write_excel_csv(df, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/4. BU Price diff.csv", na = "")

