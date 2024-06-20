library(fs)
library(readxl)
library(purrr)
library(readr)
library(dplyr)
library(stringr)


# List of multiple excel files ----
xls_files <- dir_ls("data/Variations tracking", regexp = "\\.xlsx$")


# Read multiple excel files ----
df <- xls_files %>%
  map_dfr(read_excel,
    sheet = "Seasonalized Variations",
    range = "A16:AA228",
    .id = "source"
  )


# Select columns and remove NA rows ----
df <- df %>%
  select(-c(4:6, 24:28)) %>%
  rename(
    Plant  = ...1,
    Outlet = ...2,
    Items  = ...6,
  ) %>%
  filter(!is.na(Plant))


# Remove unnecessary text ----
df <- df %>%
  mutate(
    source = str_remove(source, "_2022"),
    source = str_remove(source, ".xlsx"),
    source = str_remove(
      source,
      "data/Variations tracking/variations_tracking_242_"
    ),
  )


# Add POC information ----
poc <- read_csv("meta/POC.csv")

df <- df %>%
  left_join(poc, by = c("Plant", "Outlet"))


# Add Variations item information ----
var <- read_csv("meta/Variations items.csv")

df <- df %>%
  left_join(var, by = "Items")


# Write a CSV file ----
write_csv(df, "output/Variations trackings.csv")
