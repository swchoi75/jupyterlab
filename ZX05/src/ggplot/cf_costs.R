library(dplyr)
library(readr)
library(tidyr)
library(forcats)
library(janitor)
library(ggplot2)
library(knitr)
library(scales)


# Path
path <- "../"

# Read the data
df <- read_csv(paste0(path, "output/ZX05/CF costs.csv")) |>
  clean_names()


# Change data type
df <- df |>
  mutate(across(cost_ctr, as.character))


# Filter primary costs
df <- df |>
  filter(acc_lv2 %in% c(
    "299 Total Labor Costs",
    "465 Cost of materials",
    "535 Services In/Out"
  ))


# Select columns
id_cols <- c(
  "fy",
  "period",
  "cost_ctr",
  "gl_accounts",
  "profit_ctr",
  "fix_var",
  "department",
  "acc_lv2"
)
numeric_cols <- c(
  "plan",
  "actual",
  # "target",
  "delta_to_plan"
)

df <- df |>
  select(all_of(id_cols), all_of(numeric_cols))


# Change sign logic from RACE to SAP & from k LC to LC
df <- df |>
  mutate(
    across(numeric_cols, ~ (.x * -1e+3)),
    across(c("delta_to_plan"), ~ (.x * -1))
  )


# Summarize the data ----
df <- df |>
  group_by(pick(id_cols)) |>
  summarise(across(numeric_cols, sum)) |>
  ungroup()

top_10_negative <- df |>
  arrange(delta_to_plan) |>
  slice_head(n = 10)

top_10_positive <- df |>
  arrange(delta_to_plan) |>
  slice_tail(n = 10)

df_sum <- df |>
  group_by() |>
  summarise(across(numeric_cols, sum)) |>
  ungroup()


# Visualize the data ----
delta_chart <- function(df, y_col) {
  # Summarize the data
  df <- df |>
    group_by(df[y_col], df$acc_lv2) |>
    summarise(across(numeric_cols, sum)) |>
    ungroup()
  # Visualize the data
  plt <- ggplot(
    df,
    aes(
      x = df$delta_to_plan,
      y = fct_reorder(df[y_col], desc(df$delta_to_plan)),
      fill = df$acc_lv2
    )
  ) +
    geom_col() +
    scale_x_continuous(labels = label_number(scale = 1 / 1e+6)) +
    scale_fill_manual(values = c("#F2E500", "#4A4944", "#D7004B"))
  return(plt)
}


# Delta charts ----
p1 <- delta_chart(df, "department") +
  labs(x = "Delta to Plan in M KRW", y = "Departments")

p2 <- delta_chart(df, "cost_ctr") +
  labs(x = "Delta to Plan in M KRW", y = "Cost centers")

p1 + theme_minimal()
p2 + theme_classic()


# Data tables ----
top_10_negative

top_10_positive

bind_rows(df, df_sum)
