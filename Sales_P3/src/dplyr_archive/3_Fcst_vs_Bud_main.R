library(tidyverse)
library(readxl)
library(writexl)


# Master data ----
source("script/master_data.R")


# Process forecast data ----
source("script/Forecast_data.R")
forecast_path <-
  "data/Forecast/FC05+7/FC05+7 Cons.xlsx"
df_fcst <- process_forecast_data(forecast_path)

source("script/forecast_std costs.R")
df_fcst <- forecast_std_costs(df_fcst) # Add std costs for forecast



# Process budget data ----
source("script/budget_data.R")
budget_path <-
  "data/Plan/2023_BPR_Consolidation_20220907_CMU Material EQ update.xlsx"
df_bud <- process_budget_data(budget_path)

source("script/budget_std costs.R")
df_bud <- budget_std_costs(df_bud) # Add std costs for Budget





# Bind Actual and forecast data and write to a file ----
df <- bind_rows(
  df_fcst |> mutate(Version = "Forecast", .before = Year),
  df_bud |> mutate(Version = "Budget", .before = Year)
)

# Column addition ----
df <- df |>
  mutate(`Cost Elem.` = "NA", .after = `RecordType`)


write_csv(df, "output/Forecast and Budget.csv")





# Add additional information for forecast

source("script/report_copa_sales.R")
df <- process_copa_sales(df)

write_csv(df, "output/test cm cluster Forecast and Budget.csv")


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




# Sales P3: 3 different forecast price table ----
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






write_csv(df, "output/YTD Sales fcst vs bud.csv")
df_ytd <- df


# Sales P3: SPV mapping for 3 differenct scenario ----
df_ytd_fcst <- df_ytd |> filter(Version == "Forecast")
df_ytd_bud <- df_ytd |> filter(Version == "Budget")

map_div_e <- spv_mapping(
  df_div_e(df_ytd_fcst),
  bud_price_div_e(df), "Customer Material"
)
map_div_p <- spv_mapping(
  df_div_p(df_ytd_fcst),
  bud_price_div_p(df), "Product"
)
map_pl_cm <- spv_mapping(
  df_pl_cm(df_ytd_fcst),
  bud_price_pl_cm(df), "CM Cluster"
)

df <- bind_rows(map_div_e, map_div_p, map_pl_cm, df_ytd_bud)



# Sales P3: Price, Volume, Mix ----
source("script/price_impact_fcst_vs_bud.R")
df <- df |>
  calc_price_impact_fcst_vs_bud() |>
  missing_values_fcst_vs_bud()


source("script/price_vol_mix_fcst_vs_bud.R")
df <- df |>
  join_with_cm_ratio() |>
  delta_impact_fcst_vs_bud()

write_csv(df, "output/YTD Sales_P3 Fcst vs Bud.csv")
