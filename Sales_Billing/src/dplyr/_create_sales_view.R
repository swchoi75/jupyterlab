library(dplyr)
library(readr)
library(janitor)
library(here)


# Path
path <- here("Sales_Billing")


# Functions
filter_last_date <- function(df) {
  last_date <- last(df$date) # find the last date
  df <- df |>
    filter(df$date == last_date)
  return(df)
}


change_format <- function(df) {
  df <- df |>
    mutate(
      plant = "0180",
      sales_org = "5668",
      acc_assig_grp = case_when(
        Type == "FERT" ~ "01",
        Type == "HAWA" ~ "02",
        Type == "HALB" ~ "03",
        Type == "ROH" ~ "04",
      ),
      material_group1 = case_when(
        Customer == "HMC" ~ "183",
        Customer == "KMC" ~ "191",
        Customer == "KIA" ~ "191",
        Customer == "GM" ~ "105",
        Customer == "RSM" ~ "193",
        Customer == "Ssangyong" ~ "180",
        Customer == "SYM" ~ "180",
        Customer == "Others" ~ "199",
        Customer == "MOBIS" ~ "317",
        Customer == "Mobis" ~ "317",
        Customer == "MANDO" ~ "384",
        Customer == "Mando" ~ "384",
        Customer == "Webasto" ~ "376",
        Customer == "VT" ~ "360",
        Customer == "Conti" ~ "394",
      ),
      material_group2 = "",
      material_group3 = "",
    )
  return(df)
}


main <- function() {
  # Filenames
  input_file <- here(path, "data", "Sales View Creation Request.csv")
  output_1 <- here("C", "LSMW", "Sales_view.txt")
  output_2 <- here(path, "output", "Sales_view.txt")

  # Read data
  df <- read_csv(input_file, show_col_types = FALSE) |>
    clean_names()
  df <- read_csv("C:/Users/uiv09452/OneDrive - Vitesco Technologies/Desktop/VBA Extract/Sales View Creation Request for R/Sales View Creation Request.csv", show_col_types = FALSE) |>
    clean_names()

  # Process data
  df <- df |>
    filter_last_date() |>
    change_format() |>
    # select columns
    select(c(
      "plant",
      "sales_org",
      "material_number",
      "acc_assig_grp",
      "material_group1",
      "material_group2",
      "material_group3"
    ))

  # Write data
  write_csv(df, output_1, na = "")
  write_csv(df, output_2, na = "")
  print("Files are created")
}

main()
