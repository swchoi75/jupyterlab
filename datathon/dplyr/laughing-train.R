library(tidyverse)
library(lubridate)

pricing <- function() {
  read_csv("pricing.csv", show_col_types = FALSE)
}

department <- function() {
  read_csv("department.csv", show_col_types = FALSE) %>%
    mutate(department = str_trim(department))
}

main_data <- function() {
  read_csv("metrics.csv", show_col_types = FALSE) %>%
    left_join(department(), by = "id") %>%
    process_date_columns()
}

process_date_columns <- function(df) {
  df %>%
    mutate(
      data_timestamp = mdy_hm(data_timestamp),
      created_at = mdy_hm(created_at),
      updated_at = mdy_hm(updated_at),
      last_patch = mdy_hm(last_patch)
    )
}

text_columns <- function(df) {
  df %>%
    select(id, department, disk_size, type, size) %>%
    distinct()
}


df <- main_data()
df_text <- text_columns(df)


# 1.2 How many departments use the appliances of the Data Platform?
number_of_department <- function(df) {
  df %>%
    select(id, department) %>%
    count(department, sort = TRUE)
}
number_of_department(df_text)
count(number_of_department(df_text))


# 1.3 What is the most popular appliance size used by all departments?
# And how many of those popular sizes did you find in the whole dataset?
appliance_size <- function(df) {
  df %>%
    count(size, sort = TRUE)
}
appliance_size(df_text)


# 2.1 Which is the most popular appliance type per department?
type_by_dept <- function(df) {
  df %>%
    count(department, type, sort = TRUE) %>%
    arrange(department)
}
type_by_dept(df_text)


# 2.2 Wich appliance size had the lowest vCPU utilization
# over the full time range of the dataset based on the listed metrics?
# Calculate a value with 6 digits after zero for each metric:
vcpu_by_size <- function(df) {
  df %>%
    select(id, department, size, vcpu) %>%
    distinct() %>%
    select(size, vcpu) %>%
    group_by(size) %>%
    summarise(
      vcpu_min = min(vcpu), # why NA value shows up ?
      # vcpu_median = median(vcpu),
      # vcpu_mean = mean(vcpu)
    )
}
vcpu_by_size(df)


# 2.3 Which department has used the most appliances
#     between 15.12.2022 and 16.01.2023?
#     How many appliances did they use in this time range?
data_in_periods <- function(df) {
  df %>%
    filter(
      data_timestamp > mdy("12/15/2022") &
        data_timestamp < mdy("01/16/2023")
    )
}


appliance_in_periods <- function(df) {
  df %>%
    data_in_periods() %>%
    select(id, department) %>%
    distinct() %>%
    count(department, sort = TRUE)
}
appliance_in_periods(df)


# 2.4 What is the most expensive size of an appliance used
# in the Data Platform in terms of hours used per department?
data_usage <- function(df) {
  df %>%
    left_join(pricing(), by = "size") %>%
    select(department, size, data_timestamp, cost_per_hour) %>%
    distinct() %>%
    group_by(department, size, cost_per_hour) %>%
    summarise(data_timestamp = n())
}


calc_cost <- function(df) {
  df %>%
    data_usage() %>%
    mutate(
      cost = round(data_timestamp / 12 * cost_per_hour, 6) # check rounding
    ) %>%
    arrange(desc(department), desc(cost))
}
calc_cost(df)



# 3.1 Which fields are important to find out if an appliance is idle
# - meaning that an appliance is running but no action is performed on it?
# Sort the correct values in alphabetic order, before submitting your response.



# 3.2 Which appliances were idle and when?


# 3.3.1 How much costs did the appliances generate in the idle state?


# 3.3.2 Compared to the total cost generated overall,
# how much percent are attributed to the idle appliances?
