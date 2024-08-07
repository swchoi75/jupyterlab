library(dplyr)
library(readr)
library(stringr)
library(here)


# Path
path <- here("Investment_spending")


# Function
read_csv_file <- function(path, fc_version) {
  df <- read_csv(path, show_col_types = FALSE) |>
    mutate(version = str_replace(.data$version, "FC", fc_version))
  return(df)
}

main <- function() {
  # Filenames
  input_fc10 <- here(path, "output", "Monthly Spending FC10+2.csv")
  input_fc6 <- here(path, "output", "Monthly Spending FC6+6.csv")
  input_fc4 <- here(path, "output", "Monthly Spending FC4+8.csv")
  input_fc3 <- here(path, "output", "Monthly Spending FC3+9.csv")
  input_fc2 <- here(path, "output", "Monthly Spending FC2+10.csv")
  output_file <- here(path, "output", "Monthly Spending merged.csv")

  # Process data
  fc10 <- read_csv_file(input_fc10, "FC10+2")
  fc6 <- read_csv_file(input_fc6, "FC6+6") |> filter(str_detect(version, "FC"))
  fc4 <- read_csv_file(input_fc4, "FC4+8") |> filter(str_detect(version, "FC"))
  fc3 <- read_csv_file(input_fc3, "FC3+9") |> filter(str_detect(version, "FC"))
  fc2 <- read_csv_file(input_fc2, "FC2+10") |> filter(str_detect(version, "FC"))

  merged <- bind_rows(fc10, fc6, fc4, fc3, fc2)

  # Write data
  write_csv(merged, output_file)
  print("A file is created")
}

main()
