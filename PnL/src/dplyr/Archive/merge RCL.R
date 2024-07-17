library(fs)
library(readxl)
library(purrr)
library(dplyr)
library(stringr)
library(readr)


# Budget FX rate ----
bud_fx <- 1362 # Budget FX rate in 2022


# List of multiple excel files ----
xls_files <- dir_ls("data/RCL", regexp = "\\.xlsm$")


# Read multiple excel files ----
df <- xls_files |>
  map_dfr(read_excel,
    sheet = "RCL Input GC", skip = 4, .id = "source",
    col_types = c(
      "text", "numeric", "numeric", "numeric",
      "text", "numeric", "numeric", "numeric",
      "text", "numeric", "numeric", "numeric",
      "text", "numeric", "numeric", "numeric",
      "text", "numeric", "numeric", "numeric",
      "numeric", "numeric", "numeric"
    ),
  )


# Change GC to LC ----
df <- df |>
  mutate(across(where(is.numeric), ~ .x * bud_fx))


# Rename Columns ----
df <- df |>
  select(!c("ACT Period GC":"FC GC fx impact")) |>
  rename(
    `Period Plan`         = `Single Period BUD GC`,
    `Period Act`          = `Single Period ACT GC @ BUD fx`,
    `Delta Period`        = `Single Period ACT-BUD GC @ BUD fx`,
    `Comments Period`     = `Comment Single Period ACT-BUD Period`,
    `YTD Plan`            = `YTD BUD GC`,
    `YTD Act`             = `YTD ACT GC @ BUD fx`,
    `Delta YTD`           = `YTD ACT-BUD GC @ BUD fx`,
    `Comments YTD`        = `Comment YTD ACT-BUD`,
    `Plan`                = `FY BUD GC`,
    `FC`                  = `FC GC @ BUD fx...11`,
    `Delta to Plan`       = `FC - BUD GC @ BUD fx`,
    `Comments FC`         = `Comment FC - BUD GC`,
    `LFC`                 = `LFC GC @ BUD fx`,
    `FC_`                 = `FC GC @ BUD fx...15`,
    `Delta to LFC`        = `FC - LFC GC @ BUD fx`,
    `Comments FC changes` = `Comment FC - LFC`
  )


# Get Outlet, Plant infomration from file names using Regex ----
df <- df |>
  mutate(
    Outlet_Plant = str_extract(source, "[0-9\\_]{7,9}"),
    Outlet = str_extract(Outlet_Plant, "[0-9]{3,4}"),
    Plant = str_extract(Outlet_Plant, "[0-9]{3}$")
  ) |>
  select(!c(source, Outlet_Plant)) |>
  relocate(Outlet, Plant)


# Plant Outlet Combination ----
poc <- read_csv("meta/POC.csv", col_types = "ccc") |> # "c" as Character type
  select(!c(CU, `Profit Center`))

df <- df |>
  left_join(poc, by = c("Outlet", "Plant")) |>
  relocate(BU, `Outlet name`, `Plant name`)


# Add key column for look up RCL comments ----
df <- df |>
  tidyr::unite(Key, c("Outlet", "Plant", "RCL Item structure"),
    remove = FALSE
  ) |>
  relocate(Key, .after = last_col())


# Write a CSV file ----
write_csv(df, "output/RCL.csv", na = "")
