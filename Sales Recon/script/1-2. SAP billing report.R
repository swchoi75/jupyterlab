library(readr)
library(dplyr)
library(stringr)
library(readxl)


# Read Excel files ----
df_0180 <- read_excel("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/SAP/Billing_0180_2024_06.xlsx") %>% # Update monthly
  filter(!is.na(`Sales Organization`))

df_2182 <- read_excel("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/SAP/Billing_2182_2024_06.xlsx") %>% # Update monthly
  filter(!is.na(`Sales Organization`))

price <- read_tsv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/data/SAP/Price_2024-06-30.xls", # Update monthly
  skip = 3,
  locale = locale(encoding = "UTF-16LE"),
  col_types = cols(.default = col_character()),
) %>%
  select(-c(1, ReSt)) %>%
  rename(
    `Curr` = `Unit...9`,
    `per unit` = `Unit...10`,
    `Current Price` = `Amount`
  )


# Combine two dataframes ----
df_0180 <- df_0180 %>%
  mutate(Plant = "0180")

df_2182 <- df_2182 %>%
  mutate(Plant = "2182")

df <- bind_rows(df_0180, df_2182) %>%
  relocate(Plant, .after = `Sales Organization`)



# Change types from text to double ----
price <- price %>%
  mutate(
    `Current Price` = str_remove_all(`Current Price`, ","),
    `Current Price` = as.double(`Current Price`),
  )


# Remove duplicates in Price list to get latest prices ----
price_latest <- price %>%
  tidyr::unite("Customer_Material", Customer:Material, remove = FALSE) %>%
  filter(!duplicated(`Customer_Material`))







# Join two dataframes ----
df <- df %>%
  left_join(price_latest, by = c(
    "Sold-to Party" = "Customer",
    "Material Number" = "Material"
  ))




# Read CSV files ----
customer <- read_csv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/meta/고객명.csv") %>%
  select(c(`Sold-to Party`, `고객명`))

customer_plant <- read_csv("C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/meta/공장명.csv",
  col_types = cols(.default = col_character()),
) %>%
  select(c(`Ship-to Party`, `공장명`))


# Join two dataframes ----
df <- df %>%
  left_join(customer, by = "Sold-to Party") %>%
  #  filter(!is.na(`고객명`)) %>%
  relocate(`고객명`, .after = `Plant`)




# Join two dataframes ----
df <- df %>%
  left_join(`customer_plant`, by = "Ship-to Party") %>%
  relocate(`공장명`, .after = `Sold-to Party`)




# Special dealing for CUstomer MOBIS ----
df <- df %>%
  mutate(
    `고객명` = case_when(
      `고객명` == "MOBIS" & `Ship-to Party` == "10003814" ~ "MOBIS module",
      `고객명` == "MOBIS" & `Ship-to Party` == "10046779" ~ "MOBIS module",
      `고객명` == "MOBIS" & `Ship-to Party` == "40053559" ~ "MOBIS module",
      `고객명` == "MOBIS" & `Ship-to Party` == "40043038" ~ "MOBIS module",
      `고객명` == "MOBIS" ~ "MOBIS AS",
      TRUE ~ `고객명`
    )
  )


# Remove hyphen(-) and blank in Customer Part Number ----
df <- df %>%
  mutate(
    `Customer PN` = str_remove(`Customer Part Number`, "-"),
    `Customer PN` = str_remove(`Customer PN`, " "),
    .before = `Customer Part Number`
  )


df <- df %>%
  mutate(
    `Customer PN` = str_remove(`Customer PN`, "_"))


# Extract final customer PN as temp from Material Description ----
df <- df %>%
  mutate(temp = str_extract(`Material Description`, "[a-zA-Z0-9-]*$")) %>%
  mutate(temp = str_remove(temp, "-"))


# Special dealing for customer GM and MOBIS module----
df <- df %>%
  mutate(
    `Customer PN rev1` = case_when(
      `고객명` == "한국지엠" ~ str_remove(`Customer PN`, "P"),
      `고객명` == "MOBIS module" ~ temp,
      `Customer PN` == "392502" ~ "392502E000",
      TRUE ~ `Customer PN`
    ),
    .before = `Customer PN`
  )



# Special dealing for Inverter & others from MOBIS module----
df <- df %>%
  mutate(
    `Customer PN rev` = case_when(
      `Customer PN rev1` == "HIT100" ~ `Customer PN`,
      `Customer PN rev1` == "ME" ~ `Customer PN`,
      `Customer PN rev1` == "MV" ~ `Customer PN`,
      `Customer PN rev1` == "TSD" ~ `Customer PN`,
      `Customer PN rev1` == "cover" ~ `Customer PN`,
      `Customer PN rev1` == "NA" ~ `Customer PN`,
      `Customer PN rev1` == "V01" ~ `Customer PN`,
      `Customer PN rev1` == "V02" ~ `Customer PN`,
      `Customer PN rev1` == "V03" ~ `Customer PN`,
      `Customer PN rev1` == "366B02B" ~ `Customer PN`,
      TRUE ~ `Customer PN rev1`
    ),
    .before = `Customer PN rev1`
  )


# Special dealing for Inverter from MOBIS module----
df <- df[, -which(names(df) == "Customer PN rev1")]





# Summary of data ----
df_summary <- df %>%
  group_by(
    Plant, `고객명`, `Sold-to Party`, `Customer Name`,
    `Customer PN rev`, `Customer PN`, `Material Number`,
    `Division`, `Profit Center`, `Material Description`,
    `Customer_Material`, `Current Price`, `CnTy`, `Curr`
  ) %>%
  summarise(
    Qty = sum(`Billing Quantity`),
    Amt = sum(`Sales Amount(KRW)`)
  ) %>%
  mutate(`Avg billing price` = round(Amt / Qty, 2))


# Write UTF-8 CSV files with BOM ----
write_excel_csv(df_summary, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/1-2. SAP billing summary.csv")
write_excel_csv(price_latest, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/1-2. price_latest.csv")
write_excel_csv(df, "C:/Users/uiv09452/Vitesco Technologies/Controlling VT Korea - Documents/120. Data automation/R workspace/Sales Recon/output/1-2. SAP billing details.csv")
