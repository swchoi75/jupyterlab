library(dplyr)


# Define a custom function to calculate the forecast price ----
fcst_price <- function(df, col_name) {
  col <- col_name
  df <- df |>
    select(
      "Version",
      "Month",
      "Profit Ctr",
      col,
      "Qty",
      "Sales_LC"
    ) |>
    filter(df$Version == "Forecast") |>
    group_by(pick(
      "Version",
      "Profit Ctr",
      col
    )) |> # "Month" is excluded
    summarise(
      Qty = sum(df$Qty),
      Sales_LC = sum(df$Sales_LC),
      fcst_price = round(df$Sales_LC / df$Qty, 0),
    ) |>
    ungroup() |>
    tidyr::drop_na() |>
    select(!c(
      "Version",
      "Qty",
      "Sales_LC"
    )) |>
    mutate(`mapping key` = df[col], .before = "fcst_price")

  return(df)
}

# 3 different groups ----
df_div_e <- function(df) {
  df <- df |>
    filter(df["Division"] == "E")
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

# Create Forecast price tables for 3 different groups ----
fcst_price_div_e <- function(df) {
  df <- df |>
    df_div_e() |>
    fcst_price("Customer Material")
  return(df)
}

fcst_price_div_p <- function(df) {
  df <- df |>
    df_div_p() |>
    fcst_price("Product")
  return(df)
}

fcst_price_pl_cm <- function(df) {
  df <- df |>
    df_pl_cm() |>
    fcst_price("CM Cluster")
  return(df)
}

# mapping between between actual and forecast ----
spv_mapping <- function(df, fcst_price, col_name) {
  df <- df |>
    left_join(fcst_price, by = c("Profit Ctr", col_name))
  return(df)
}
