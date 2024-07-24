library(dplyr)
library(readr)


join_with_cm_ratio <- function(df) {
  bud_cm_ratio <- read_csv("meta/Budget Contribution Margin ratio.csv",
    show_col_types = FALSE
  ) |>
    select(c(
      "Profit Ctr",
      "CM ratio"
    ))

  df <- df |>
    left_join(bud_cm_ratio, by = "Profit Ctr")
  return(df)
}


delta_impact_fcst_vs_bud <- function(df) {
  df <- df |>
    mutate(
      # Sales
      `delta Sales` = ifelse(df$Version == "Forecast",
        df$Sales_LC,
        df$Sales_LC * -1
      ),
      `delta Sales Price` = ifelse(df$Version == "Forecast",
        df$price_impact,
        df$price_impact * -1
      ),
      `delta Sales Volume` =
        df["delta Sales"] - df["delta Sales Price"],

      # Margin
      `delta Margin` = ifelse(df$Version == "Forecast",
        df$Sales_LC - df$STD_Costs,
        (df$Sales_LC - df$STD_Costs) * -1
      ),
      `delta Margin Price` = df["delta Sales Price"],
      `delta Margin Volume` =
        df["delta Sales Volume"] * df["CM ratio"] / 100,
      `delta Margin Mix` = df["delta Margin"] -
      df["delta Margin Price"] - df["delta Margin Volume"]
    )
  return(df)
}
