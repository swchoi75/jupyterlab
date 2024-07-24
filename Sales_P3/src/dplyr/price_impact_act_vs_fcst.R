library(dplyr)
library(tidyr)


# Calculate Sales Price impact
calc_price_impact_act_vs_fcst <- function(df) {
  df <- df |>
    mutate(
      act_price = ifelse(df$Version == "Forecast" |
          df$Qty == 0, 0,
        round(df$Sales_LC / df$Qty, 0)
      ),
      price_diff = ifelse(df$Qty == 0, 0,
        df$act_price - df$fcst_price
      ),
      price_impact = case_when(

        # Business logic
        df["CM Cluster"] == "OES" ~ 0, # OES
        df$Product == "A2C1797520201" ~ 0, # Kappa HEV adjustment
        df$Product == "A2C1636530101" ~ 0, # Kappa HEV adjustment

        # General formula
        df$Qty == 0 ~ df$Sales_LC,
        is.na(df$Qty) ~ df$Sales_LC,
        TRUE ~ df$Qty * df$price_diff
      ),
      price_impact_ratio = df$price_impact / df$Sales_LC * 100,
    )
  return(df)
}

# Replace missing values with zero for numeric columns
missing_values_act_vs_fcst <- function(df) {
  df <- df |>
    mutate(
      Qty = replace_na(df$Qty, 0),
      Sales_LC = replace_na(df$Sales_LC, 0),
      STD_Costs = replace_na(df$STD_Costs, 0),
      `Standard Price` = replace_na(df["Standard Price"], 0),
      fcst_price = replace_na(df$fcst_price, 0),
      act_price = replace_na(df$act_price, 0),
      price_diff = replace_na(df$price_diff, 0),
      price_impact = replace_na(df$price_impact, 0),
      price_impact_ratio = replace_na(df$price_impact_ratio, 0),
    )
  return(df)
}
