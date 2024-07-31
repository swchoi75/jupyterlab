library(dplyr)
library(readxl)
library(writexl)


# Master data ----
source("script/master_data.R")


# Process actual data ----
kappa_hev <- function(path) {
  read_excel(path, sheet = "format") |>
    mutate(STD_Costs = ifelse(df["D/C"] == 40,
      df$STD_Costs,
      df$STD_Costs * -1
    )) |>
    select(!c("D/C"))
}

kappa_path <- "data/Actual/Kappa HEV adj_costs.xlsx"
kappa_cost <- kappa_hev(kappa_path)

source("script/actual_data.R")
copa_path <- "db/COPA_2023.csv"
df_act <- process_actual_data(copa_path)

df_act <- bind_rows(df_act, kappa_cost) # Add Kappa HEV adjustment (Costs)



# Process budget data ----
source("script/budget_data.R")
budget_path <-
  "data/Plan/2023_BPR_Consolidation_20220907_CMU Material EQ update.xlsx"
df_bud <- process_budget_data(budget_path)

source("script/budget_std costs.R")
df_bud <- budget_std_costs(df_bud) # Add std costs for Budget



# Bind actual and budget data and write to a file ----
df <- bind_rows(
  df_act |> mutate(Version = "Actual", .before = Year),
  df_bud |> mutate(Version = "Budget", .before = Year)
)
write_csv(df, "output/COPA Sales and Budget.csv")



# Add additional information
source("script/report_copa_sales.R")
df <- process_copa_sales(df)


# Check missing master data and write to files ----
source("script/missing_master.R")
write_csv(
  missing_customer_center(df),
  "meta/missing CC_2023.csv"
)
write_csv(
  missing_gl_accounts(df),
  "meta/missing GL.csv"
)
write_csv(
  missing_customer_material(df),
  "meta/missing Customer Material.csv"
)
write_xlsx(
  missing_material_master(df) |>
    filter(`Profit Ctr` != "50802-018"),
  "meta/missing Material_0180.xlsx",
  col_names = FALSE
)
write_xlsx(
  missing_material_master(df) |>
    filter(`Profit Ctr` == "50802-018"),
  "meta/missing Material_2182.xlsx",
  col_names = FALSE
)
write_csv(
  missing_product_hierarchy(df),
  "meta/missing PH.csv"
)



# Sales P3: 3 different budget price table ----
source("script/price_budget.R")
write_csv(
  bud_price_div_e(df),
  "output/bud_price_div_e.csv"
)
write_csv(
  bud_price_div_p(df),
  "output/bud_price_div_p.csv"
)
write_csv(
  bud_price_pl_cm(df),
  "output/bud_price_pl_cm.csv"
)



# Sales P3: YTD Sales ----
sales_ytd <- function(df) {
  df |>
    filter(df$Version == "Actual")
  last_month <- last(df_act$Month)
  # Filter YTD
  df |>
    filter(df$Month <= last_month + 1)
}

df_ytd <- sales_ytd(df)
write_csv(df_ytd, "output/YTD Sales.csv")


# Sales P3: SPV mapping for 3 differenct scenario ----
df_ytd_act <- df_ytd |> filter(Version == "Actual")
df_ytd_bud <- df_ytd |> filter(Version == "Budget")

map_div_e <- spv_mapping(
  df_div_e(df_ytd_act),
  bud_price_div_e(df), "Customer Material"
)
map_div_p <- spv_mapping(
  df_div_p(df_ytd_act),
  bud_price_div_p(df), "Product"
)
map_pl_cm <- spv_mapping(
  df_pl_cm(df_ytd_act),
  bud_price_pl_cm(df), "CM Cluster"
)

df <- bind_rows(map_div_e, map_div_p, map_pl_cm, df_ytd_bud)


# Sales P3: Price, Volume, Mix ----
source("script/price_impact.R")
df <- df |>
  calculate_price_impact() |>
  replace_missing_values()


source("script/price_vol_mix.R")
df <- df |>
  join_with_cm_ratio() |>
  delta_impact()
write_csv(df, "output/YTD Sales_P3.csv")
