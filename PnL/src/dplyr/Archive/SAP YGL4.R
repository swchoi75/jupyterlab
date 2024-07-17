library(fs)
library(purrr)
library(readr)
library(dplyr)
library(stringr)


# List of multiple excel files ----
txt_files <- dir_ls("data/SAP YGL4", regexp = ".txt")


# Read multiple tab-delimited files ----
df <- txt_files |>
  map_dfr(read_tsv,
          skip = 32,
          locale = locale(encoding = "UTF-16LE"),
          col_types = cols(.default = col_character()),
          .id = "source"
  ) |>
  select(source, `OneGL B/S + P/L`, `01`:`12`)


# Change column names ----
df <- df |>
  rename(
    Items = source,
    PrCr = `OneGL B/S + P/L`,
    Jan = `01`, Feb = `02`, Mar = `03`, Apr = `04`, May = `05`, Jun = `06`,
    Jul = `07`, Aug = `08`, Sep = `09`, Oct = `10`, Nov = `11`, Dec = `12`
  )

df <- df |>
  mutate(
    Items = str_remove(Items, "data/SAP YGL4/"),
    Items = str_remove(Items, ".txt"),
    PrCr = str_extract(PrCr, "[0-9\\-]{8,9}"),
  )


# Remove thousand separators "," and Convert types ----
df <- df |>
  mutate(
    across(c(Jan:Dec), ~ str_remove_all(.x, ",")),
    across(c(Jan:Dec), as.double),
    across(c(Jan:Dec), ~ tidyr::replace_na(.x, 0)),
  )


# Change sign logic etc ----
df <- df |>
  mutate(across(Jan:Dec, ~ .x / -10^6)) |> # Sign logic change from SAP to RACE
  tidyr::drop_na()


# Write a CSV file ----
write_csv(df, "output/SAP YGL4 P&L.csv")
