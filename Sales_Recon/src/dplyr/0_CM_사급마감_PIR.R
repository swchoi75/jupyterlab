library(dplyr)
library(readr)
library(writexl)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Recon")


# Functions
rename_columns <- function(df) {
  df <- df |>
    rename(
      "pir_net_price" = "x15",
      "sa_net_price" = "x23",
      "unit" = "x26",
      "sa_valid_from" = "x36",
      "pir_valid_from" = "info_record_40",
    )
  return(df)
}


main <- function() {
  # Variables
  source(here(path, "src", "dplyr", "common_variable.R"))

  # Filenames
  input_file <- here(path, "data", "VAN CM", glue("SA_{year}{month}.xls"))
  output_1 <- here(path, "data", "VAN CM", "SA_LS Automotive.xlsx")
  output_2 <- here(path, "data", "VAN CM", "SA_MOBASE.xlsx")

  # Read data
  df <- read_tsv(input_file,
    skip = 9,
    locale = locale(encoding = "UTF-16LE"),
    show_col_types = FALSE,
  ) |> clean_names(ascii = FALSE)

  # Process data
  df <- df |> 
    rename_columns() |>
    # select columns
    select(c(
      "vendor",
      "vendor_name",
      "material_no",
      "mtye",
      "sa_net_price",
      "unit",
      "sa_valid_from",
      "sa_number"
    ))

  ## Filter dataframe
  df <- df |>
    filter(.data$sa_net_price != 0)

  df_ls <- df |>
    filter(.data$vendor == "9139976")

  df_mo <- df |>
    filter(.data$vendor == "9082855")

  # Write data
  write_xlsx(df_ls, output_1)
  write_xlsx(df_mo, output_2)
  print("Files are created")
}

main()
