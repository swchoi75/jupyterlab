library(readr)
library(dplyr)
library(writexl)


# Read a tab-delimited file ----
df <- read_tsv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/VAN CM/SA_202406.xls",
  skip = 9,
  locale = locale(encoding = "UTF-16LE"),
  show_col_types = FALSE,
)


# Select and change column names ----
df <- df %>%
  rename(
    `PIR Net Price` = `...15`,
    `S/A Net Price` = `...23`,
    Unit = `...26`,
    `S/A valid from` = `...36`,
    `PIR valid from` = `Info Record...40`,
  ) %>%
  select(
    Vendor, `Vendor Name`, `Material No`, `Mtye`,
    `S/A Net Price`, Unit, `S/A valid from`, `SA #`
  ) %>%
  tidyr::separate(`SA #`, into = c("S/A"), sep = "\\t")



# Filter dataframe ----
df <- df %>%
  filter(`S/A Net Price` != 0)

df_LS <- df %>%
  filter(Vendor == "9139976")

df_MO <- df %>%
  filter(Vendor == "9082855")


# Write Excel files ----
write_xlsx(df_LS, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/VAN CM/SA_LS Automotive.xlsx")
write_xlsx(df_MO, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/VAN CM/SA_MOBASE.xlsx")
