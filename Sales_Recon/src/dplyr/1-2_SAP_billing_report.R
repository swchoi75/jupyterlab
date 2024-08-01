library(dplyr)
library(readr)
library(stringr)
library(readxl)
library(janitor)
library(here)
library(glue)


# Path
path <- here("Sales_Recon")


# Functions
change_data_type <- function(df) {
  # Change types from text to double
  df <- df |>
    mutate(
      `Current Price` = str_remove_all(.data$`Current Price`, ","),
      `Current Price` = as.double(.data$`Current Price`),
    )
  return(df)
}


get_latest_price <- function(df) {
  # Remove duplicates in Price list to get latest prices
  df <- df |>
    tidyr::unite("Customer_Material", c("Customer":"Material"),
      remove = FALSE
    ) |>
    filter(!duplicated(.data$`Customer_Material`))
  return(df)
}


process_mobis <- function(df) {
  # Special dealing for CUstomer MOBIS
  df <- df |>
    mutate(
      고객명 = case_when(
        고객명 == "MOBIS" & `Ship-to Party` == "10003814" ~ "MOBIS module",
        고객명 == "MOBIS" & `Ship-to Party` == "10046779" ~ "MOBIS module",
        고객명 == "MOBIS" & `Ship-to Party` == "40053559" ~ "MOBIS module",
        고객명 == "MOBIS" & `Ship-to Party` == "40043038" ~ "MOBIS module",
        고객명 == "MOBIS" ~ "MOBIS AS",
        TRUE ~ 고객명
      )
    )
  return(df)
}


process_customer_pn <- function(df) {
  # Remove hyphen(-) and blank in Customer Part Number
  df <- df |>
    mutate(
      `Customer PN` = str_remove(.data$`Customer Part Number`, "-"),
      `Customer PN` = str_remove(.data$`Customer PN`, " "),
      .before = "Customer Part Number"
    ) |>
    mutate(
      `Customer PN` = str_remove(.data$`Customer PN`, "_")
    )
  return(df)
}


extract_final_customer_pn <- function(df) {
  # Extract final customer PN as temp from Material Description
  df <- df |>
    mutate(temp = str_extract(
      .data$`Material Description`, "[a-zA-Z0-9-]*$"
    )) |>
    mutate(temp = str_remove(.data$temp, "-"))
  return(df)
}


process_gm_mobis <- function(df) {
  # Special dealing for customer GM and MOBIS module
  df <- df |>
    mutate(
      `Customer PN rev1` = case_when(
        고객명 == "한국지엠" ~ str_remove(`Customer PN`, "P"),
        고객명 == "MOBIS module" ~ temp,
        `Customer PN` == "392502" ~ "392502E000",
        TRUE ~ `Customer PN`
      ),
      .before = "Customer PN"
    )
  return(df)
}


process_inverter <- function(df) {
  # Special dealing for Inverter & others from MOBIS module
  df <- df |>
    mutate(
      `Customer PN rev` = case_when(
        `Customer PN rev1` == "HIT100" ~ `Customer PN`,
        `Customer PN rev1` == "ME" ~ `Customer PN`,
        `Customer PN rev1` == "MV" ~ `Customer PN`,
        `Customer PN rev1` == "TSD" ~ `Customer PN`,
        `Customer PN rev1` == "cover" ~ `Customer PN`,
        `Customer PN rev1` == "NA" ~ `Customer PN`,
        `Customer PN rev1` == "V01" ~ `Customer PN`,
        `Customer PN rev1` == "V02" ~ `Customer PN`,
        `Customer PN rev1` == "V03" ~ `Customer PN`,
        `Customer PN rev1` == "366B02B" ~ `Customer PN`,
        TRUE ~ `Customer PN rev1`
      ),
      .before = "Customer PN rev1"
    )

  df <- df[, -which(names(df) == "Customer PN rev1")]

  return(df)
}


summary_data <- function(df) {
  # Summary of data
  df <- df |>
    group_by(across(c(
      "Plant", "고객명", "Sold-to Party", "Customer Name",
      "Customer PN rev", "Customer PN", "Material Number",
      "Division", "Profit Center", "Material Description",
      "Customer_Material", "Current Price", "CnTy", "Curr"
    ))) |>
    summarise(
      Qty = sum(.data$`Billing Quantity`),
      Amt = sum(.data$`Sales Amount(KRW)`)
    ) |>
    mutate(`Avg billing price` = round(.data$Amt / .data$Qty, 2))
  return(df)
}


main <- function() {
  # Variables
  year <- "2024"
  month <- "06"
  day <- "30"

  # Filenames
  input_1 <- here(
    path, "data", "SAP", glue("Billing_0180_{year}_{month}.xlsx")
  )
  input_2 <- here(
    path, "data", "SAP", glue("Billing_2182_{year}_{month}.xlsx")
  )
  input_3 <- here(
    path, "data", "SAP", glue("Price_{year}-{month}-{day}.xls")
  )

  meta_1 <- here(path, "meta", "고객명.csv")
  meta_2 <- here(path, "meta", "공장명.csv")

  output_1 <- here(path, "output", "1-2. SAP billing summary.csv")
  output_2 <- here(path, "output", "1-2. price_latest.csv")
  output_3 <- here(path, "output", "1-2. SAP billing details.csv")

  # Read data
  df_0180 <- read_excel(input_1) |> filter(!is.na(.data$`Sales Organization`))
  df_2182 <- read_excel(input_2) |> filter(!is.na(.data$`Sales Organization`))
  price <- read_tsv(input_3,
    skip = 3,
    locale = locale(encoding = "UTF-16LE"),
    col_types = cols(.default = col_character()),
  ) |>
    select(!c(1, "ReSt")) |>
    rename(
      "Curr" = "Unit...9",
      "per unit" = "Unit...10",
      "Current Price" = "Amount"
    )

  customer <- read_csv(meta_1, show_col_types = FALSE) |>
    select(c("Sold-to Party", "고객명"))

  customer_plant <- read_csv(meta_2,
    col_types = cols(.default = col_character()),
  ) |>
    select(c("Ship-to Party", "공장명"))

  # Process data
  df_0180 <- df_0180 |> mutate(Plant = "0180")
  df_2182 <- df_2182 |> mutate(Plant = "2182")
  df <- bind_rows(df_0180, df_2182) |>
    relocate("Plant", .after = "Sales Organization")

  price <- price |> change_data_type()
  price_latest <- price |> get_latest_price()

  ## Join dataframes
  df <- df |>
    ##
    left_join(price_latest, by = c(
      "Sold-to Party" = "Customer",
      "Material Number" = "Material"
    )) |>
    ##
    left_join(customer, by = "Sold-to Party") |>
    #  filter(!is.na(`고객명`)) |>
    relocate("고객명", .after = "Plant") |>
    ##
    left_join(customer_plant, by = "Ship-to Party") |>
    relocate("공장명", .after = "Sold-to Party")

  df <- df |>
    process_mobis() |>
    process_customer_pn() |>
    extract_final_customer_pn() |>
    process_gm_mobis() |>
    process_inverter()

  df_summary <- df |> summary_data()

  # Write data
  write_excel_csv(df_summary, output_1)
  write_excel_csv(price_latest, output_2)
  write_excel_csv(df, output_3)
  print("Files are created")
}

main()
