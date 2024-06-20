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
df <- read_csv(paste0(path, "output/ZX05/PL fix costs.csv")) %>%
  clean_names()


# Change data type
df <- df %>%
  mutate(across(cost_ctr, as.character))


# Filter primary costs
df <- df %>%
  filter(race_item %in% c(
    "PE production",
    "PE materials management",
    "PE plant administration"
  ))


# Select columns
id_cols <- c(
  "cost_ctr",
  "gl_accounts",
  "profit_ctr",
  "fix_var_cc",
  "department",
  "acc_lv2",
  # Add
  "bu",
  "division",
  "plant_name",
  "outlet_name",
  "ce_text",
  "race_item"
)
numeric_cols <- c(
  "plan",
  "actual",
  # "target",
  "delta_to_plan"
)

df <- df %>%
  select(all_of(id_cols), all_of(numeric_cols))


# Change sign logic from RACE to SAP & from k LC to LC
df <- df %>%
  mutate(
    across(numeric_cols, ~ (.x * -1e+3)),
    across(c("delta_to_plan"), ~ (.x * -1))
  )


# Summarize the data ----
df <- df %>%
  group_by(pick(id_cols)) %>%
  summarise(across(numeric_cols, sum)) %>%
  ungroup()

top_10_negative <- df %>%
  arrange(delta_to_plan) %>%
  slice_head(n = 10)

top_10_positive <- df %>%
  arrange(delta_to_plan) %>%
  slice_tail(n = 10)

df_sum <- df %>%
  group_by() %>%
  summarise(across(numeric_cols, sum)) %>%
  ungroup()


# Visualize the data ----
delta_chart <- function(df, y_col) {
  # Summarize the data
  df <- df %>%
    group_by(.data[[y_col]], .data[["division"]]) %>%
    summarise(across(numeric_cols, sum)) %>%
    ungroup()
  # Visualize the data
  plt <- ggplot(
    df,
    aes(
      x = .data[["delta_to_plan"]],
      y = fct_reorder(.data[[y_col]], desc(.data[["delta_to_plan"]])),
      fill = .data[["division"]]
    )
  ) +
    geom_col() +
    scale_x_continuous(labels = label_number(scale = 1 / 1e+6)) +
    scale_fill_manual(values = c("#64AF59", "#006EAA"))
  return(plt)
}


# Delta charts ----
p1 <- delta_chart(df, "outlet_name") +
  labs(x = "Delta to Plan in M KRW", y = "Productlines")

p2 <- delta_chart(df, "ce_text") +
  labs(x = "Delta to Plan in M KRW", y = "Cost Elements")

p1 + theme_minimal()
p2 + theme_classic()


# Data tables ----
top_10_negative

top_10_positive

bind_rows(df, df_sum)
