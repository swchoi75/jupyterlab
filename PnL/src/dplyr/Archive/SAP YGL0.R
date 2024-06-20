library(fs)
library(purrr)
library(readr)
library(dplyr)
library(stringr)


# List of multiple excel files ----
txt_files <- dir_ls("data/SAP YGL0", regexp = "5080")


# Read multiple tab-delimited files ----
df <- txt_files %>%
  map_dfr(read_tsv,
          skip = 32,
          locale = locale(encoding = "UTF-16LE"),
          col_types = cols(.default = col_character()),
          .id = "source"
  ) %>%
  select(source, `OneGL B/S + P/L`, `01`:`12`)


# Change column names ----
df <- df %>%
  rename(
    PrCr = source,
    Item = `OneGL B/S + P/L`,
    Jan = `01`, Feb = `02`, Mar = `03`, Apr = `04`, May = `05`, Jun = `06`,
    Jul = `07`, Aug = `08`, Sep = `09`, Oct = `10`, Nov = `11`, Dec = `12`
  ) %>%
  # Extract RACE account, G/L accounts in number format ----
  mutate(Key = str_extract(Item, "[0-9]+|^K[0-9]+|^P[0-9]+"))


# Remove thousand separators "," and Convert types ----
df <- df %>%
  mutate(
    PrCr = str_extract(PrCr, "[0-9\\-]{8,9}"),
    across(c(Jan:Dec), ~ str_remove_all(.x, ",")),
    across(c(Jan:Dec), as.double),
    across(c(Jan:Dec), ~ tidyr::replace_na(.x, 0)),
  )


# Read a CSV rile as character columns ----
df_lookup <- read_csv("meta/Lookup_table.csv",
  col_types = cols(.default = col_character()),
) %>%
  # Extract RACE account, G/L account in number format ----
  mutate(Key = str_extract(Key, "[0-9]+|^K[0-9]+|^P[0-9]+"))


# Join two data tables ----
df <- df %>%
  left_join(df_lookup, by = "Key") %>%
  filter(A != "") %>%
  select(PrCr, A:D, Jan:Dec)


# Change sign logic from SAP to RACE ----
df <- df %>%
  mutate(across(Jan:Dec, ~ .x / -10^3))


### Write a CSV file ###
write_csv(df, "output/SAP YGL0 P&L.csv")
