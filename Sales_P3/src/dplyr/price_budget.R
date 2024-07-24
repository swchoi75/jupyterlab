library(dplyr)


# Define a custom function to calculate the budget price ----
bud_price <- function(df, col_name) {
  col <- col_name
  df <- df |>
    select(c(
      "Version",
      "Month",
      "Profit Ctr",
      col,
      "Qty",
      "Sales_LC"
    )) |>
    filter(df$Version == "Budget") |>
    group_by(pick(
      "Version",
      "Profit Ctr",
      col,
    )) |> # "Month" is excluded
    summarise(
      Qty = sum(df$Qty),
      Sales_LC = sum(df$Sales_LC),
      bud_price = round(df$Sales_LC / df$Qty, 0),
    ) |>
    ungroup() |>
    tidyr::drop_na() |>
    select(!c("Version", "Qty", "Sales_LC")) |>
    mutate(`mapping key` = df[col], .before = "bud_price")

  return(df)
}

# 3 different groups ----
df_div_e <- function(df) {
  df <- df |>
    filter(df$Division == "E")
  return(df)
}

df_div_p <- function(df) {
  df <- df |>
    filter(df["Outlet name"] %in% c("PL EAC", "PL HYD", "PL MES"))
  return(df)
}

df_pl_cm <- function(df) {
  df <- df |>
    filter(df["Outlet name"] %in% c("PL CM CCN", "PL CM CVS", "PL CM PSS"))
  return(df)
}

# Create budget price tables for 3 different groups ----
bud_price_div_e <- function(df) {
  df <- df |>
    df_div_e() |>
    bud_price("Customer Material")
  return(df)
}

bud_price_div_p <- function(df) {
  df <- df |>
    df_div_p() |>
    bud_price("Product")
  return(df)
}

bud_price_pl_cm <- function(df) {
  df <- df |>
    df_pl_cm() |>
    bud_price("CM Cluster")
  return(df)
}

# mapping between between actual and budget ----
spv_mapping <- function(df, bud_price, col_name) {
  df <- df |>
    left_join(bud_price, by = c("Profit Ctr", col_name))
  return(df)
}
