library(readr)
library(dplyr)
library(stringr)


# Read data files ----
df <- read_csv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/2-2. Join Tax invoice and SAP billing.csv",
  col_types = cols(.default = col_character()),
)

df_1 <- read_csv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/1-1. Tax invoice all.csv",
  col_types = cols(.default = col_character()),
)

df_2 <- read_csv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/1-2. SAP billing summary.csv",
  col_types = cols(.default = col_character()),
)


# Select columns and Remove duplicate rows ----
df_1 <- df_1 %>%
  select(c(`고객명`:`Customer PN rev`)) %>%
  tidyr::unite("temp", `고객명`, `Sold-to Party`, `Customer PN rev`,
    sep = "_", remove = FALSE
  )

df_2 <- df_2 %>%
  filter(!is.na(`고객명`)) %>%
  select(c(Plant:Curr)) %>%
  tidyr::unite("temp", `고객명`, `Sold-to Party`, `Customer PN rev`,
    sep = "_", remove = FALSE
  )


# Remove duplicate rows ----
df_1a <- df_1 %>%
  filter(!duplicated(temp)) %>%
  select(-c(`temp`))

df_2a <- df_2 %>%
  filter(!duplicated(temp)) %>%
  select(-c(`temp`))

df_2b <- df_2 %>%
  filter(duplicated(temp)) %>%
  select(-c(`temp`))


# Join two dataframes ----
df <- df %>%
  left_join(df_2a, by = c("고객명", "Sold-to Party", "Customer PN rev"))

df <- df %>%
  left_join(df_1a, by = c("고객명", "Sold-to Party", "Customer PN rev"))


# bind_rows ----
df <- df %>%
  bind_rows(df_2b)


# Arrange rows by column values ----
df <- df %>%
  arrange(
    `고객명`, `Sold-to Party`, `Customer PN rev`,
    `Plant`, `Profit Center`, `Material Number`
  )


# Write UTF-8 CSV files with BOM ----
write_excel_csv(df, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/2-3. 입출고 비교.csv", na = "0")
