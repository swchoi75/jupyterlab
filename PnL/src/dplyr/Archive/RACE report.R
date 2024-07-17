library(readxl)
library(readr)
library(dplyr)
library(stringr)


# Function definition & Read Excel files ----

read_race <- function(lc_gc, filename) {
  read_excel(filename, sheet = "Query", skip = 11) |>
    rename(
      `FS item description` = `...2`,
      `CU name` = `...4`,
      `Plant name` = `...6`,
      `Outlet name` = `...8`
    ) |>
    rename_with(~ gsub("\r\nACT", "", .x)) |>
    mutate(Currency = lc_gc) |>
    relocate(Currency)
}

lc <- read_race("LC", "data/RACE/Analysis FS Item Hierarchy for CU 698_LC.xlsx")
gc <- read_race("GC", "data/RACE/Analysis FS Item Hierarchy for CU 698_GC.xlsx")


outlet <- read_excel("meta/New outlet.xlsx",
  range = cell_cols("A:F")
) |>
  select(!c("Outlet name"))



# Combine dataframes ----
df <- bind_rows(lc, gc)


# Add new Outlet information ----
df <- df |>
  left_join(outlet, by = "Outlet") |>
  relocate(`Division`, `BU`, `new Outlet`, `new Outlet name`, .after = `Outlet name`)


# Profit and Loss statement ----
pl <- df |>
  select(!c("Period 0", "YTD 0":"YTD 12")) |>
  filter(str_detect(`Financial Statement Item`, "^3|^CO"))


# Balance Sheet statement ----
bs <- df |>
  select(!c("Period 0":"Period 12")) |>
  filter(str_detect(`Financial Statement Item`, "^1|^2"))


# Write a CSV file ----
write_csv(pl, "output/RACE Profit and Loss.csv", na = "0")
write_csv(bs, "output/RACE Balance sheet.csv", na = "0")
