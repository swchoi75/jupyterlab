library(fs)
library(readxl)
library(purrr)
library(readr)
library(dplyr)
library(stringr)


# List of multiple excel files ----
xls_files <- dir_ls("data/Inventory devaluation", regexp = "\\.xlsx$")


# Read multiple excel files ----
df <- xls_files %>%
  map_dfr(read_excel,
    sheet = "Total devaluation",
    range = "B4:AM30000",
    .id = "source"
  )


# Remove NA rows ----
df <- df %>%
  filter(!is.na(Plant))


# Capture Year and Month ----
df <- df %>%
  mutate(
    source = str_extract(source, "[0-9.-]+"),
  )

# Filter the dataset ----
df <- df %>%
  filter(`Monthly Impact` > 10^7 | `Monthly Impact` < -10^7)


# Write a CSV file ----
write_csv(df, "output/Inventory devaluation.csv")
