library(dplyr)
library(readr)
library(writexl)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Recon")


# Functions
select_change_col_names <- function(df) {
  df <- df |>
    rename(
      "PIR Net Price" = "...15",
      "S/A Net Price" = "...23",
      "Unit" = "...26",
      "S/A valid from" = "...36",
      "PIR valid from" = "Info Record...40",
    ) |>
    select(c(
      "Vendor",
      "Vendor Name",
      "Material No",
      "Mtye",
      "S/A Net Price",
      "Unit",
      "S/A valid from",
      "SA #"
    )) |>
    tidyr::separate(.data$`SA #`, into = c("S/A"), sep = "\\t")
  return(df)
}


main <- function() {
  # Variables
  year <- "2024"
  month <- "06"

  # Filenames
  input_file <- here(path, "data", "VAN CM", glue("SA_{year}{month}.xls"))
  output_1 <- here(path, "data", "VAN CM", "SA_LS Automotive.xlsx")
  output_2 <- here(path, "data", "VAN CM", "SA_MOBASE.xlsx")

  # Read data
  df <- read_tsv(input_file,
    skip = 9,
    locale = locale(encoding = "UTF-16LE"),
    show_col_types = FALSE,
  )

  # Process data
  df <- df |> select_change_col_names()

  ## Filter dataframe
  df <- df |>
    filter(.data$`S/A Net Price` != 0)

  df_ls <- df |>
    filter(.data$Vendor == "9139976")

  df_mo <- df |>
    filter(.data$Vendor == "9082855")

  # Write data
  write_xlsx(df_ls, output_1, na = "")
  write_xlsx(df_mo, output_2, na = "")
  print("Files are created")
}

main()
