library(readr)
library(dplyr)
library(stringr)


# Read data files ----
df <- read_csv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/2-1. Join Customer PN.csv",
  col_types = cols(.default = col_character()),
)

df_1 <- read_csv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/1-1. Tax invoice all.csv",
  col_types = cols(.default = col_character()),
) %>%
  mutate(
    across(c(`입고수량`:`서열비`), as.double),
  )

df_2 <- read_csv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/1-2. SAP billing summary.csv",
  col_types = cols(.default = col_character()),
) %>%
  mutate(
    across(c(`Current Price`, `Qty`:`Avg billing price`), as.double),
  )


# Select columns ----
df_1 <- df_1 %>%
  select(`고객명`, `Sold-to Party`, `Customer PN rev`, `입고수량`:`서열비`)

df_2 <- df_2 %>%
  select(`고객명`, `Sold-to Party`, `Customer PN rev`, Qty, Amt) %>%
  filter(!is.na(`고객명`))


# Join two dataframes ----
df <- df %>%
  left_join(df_1, by = c(
    "고객명", "Sold-to Party", "Customer PN rev"
  ))


# Summary of data ----
df <- df %>%
  group_by(
    `고객명`, `Sold-to Party`, `Customer PN rev`
  ) %>%
  summarise(
    `입고Q` = sum(`입고수량`),
    `입고금액` = sum(`입고금액`),
    `포장비` = sum(`포장비`),
    `단가소급` = sum(`단가소급`),
    `관세정산` = sum(`관세정산`),
    `Sample` = sum(`Sample`),
    # to remove 0 or NA values
    `Glovis Price` = mean(`Glovis Price`[`Glovis Price` > 0], na.rm = TRUE),
    `서열비` = sum(`서열비`),
     )


df_2 <- df_2 %>%
  group_by(
    `고객명`, `Sold-to Party`, `Customer PN rev`
  ) %>%
  summarise(
    `출고Q` = sum(Qty),
    `출고금액` = sum(Amt),
  )


# Join two dataframes ----
df <- df %>%
  left_join(df_2, by = c(
    "고객명", "Sold-to Party", "Customer PN rev"
  ))


# Write UTF-8 CSV files with BOM ----
write_excel_csv(df, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/2-2. Join Tax invoice and SAP billing.csv",
  na = "0"
)
