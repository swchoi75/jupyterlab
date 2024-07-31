library(dplyr)
source("script/master_data.R")


budget_std_costs <- function(df) {
  mat <- material_master() |>
    select(c(
      "Product",
      "Profit Ctr",
      "Standard Price"
    ))

  df <- df |>
    left_join(mat, by = c("Profit Ctr", "Product")) |>
    mutate(`STD_Costs` = df$Qty * df["Standard Price"]) |>
    select(!c("Standard Price"))

  return(df)
}
