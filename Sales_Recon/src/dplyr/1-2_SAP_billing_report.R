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
      current_price = str_remove_all(.data$current_price, ","),
      current_price = as.double(.data$current_price),
    )
  return(df)
}


get_latest_price <- function(df) {
  # Remove duplicates in Price list to get latest prices
  df <- df |>
    tidyr::unite("customer_material", c("customer":"material"),
      remove = FALSE
    ) |>
    filter(!duplicated(.data$customer_material))
  return(df)
}


process_mobis <- function(df) {
  # Special dealing for CUstomer MOBIS
  df <- df |>
    mutate(
      고객명 = case_when(
        고객명 == "MOBIS" & ship_to_party == "10003814" ~ "MOBIS module",
        고객명 == "MOBIS" & ship_to_party == "10046779" ~ "MOBIS module",
        고객명 == "MOBIS" & ship_to_party == "40053559" ~ "MOBIS module",
        고객명 == "MOBIS" & ship_to_party == "40043038" ~ "MOBIS module",
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
      customer_pn = str_remove(.data$customer_part_number, "-"),
      customer_pn = str_remove(.data$customer_pn, " "),
      .before = "customer_part_number"
    ) |>
    mutate(
      customer_pn = str_remove(.data$customer_pn, "_")
    )
  return(df)
}


extract_final_customer_pn <- function(df) {
  # Extract final customer PN as temp from Material Description
  df <- df |>
    mutate(temp = str_extract(
      .data$material_description, "[a-zA-Z0-9-]*$"
    )) |>
    mutate(temp = str_remove(.data$temp, "-"))
  return(df)
}


process_gm_mobis <- function(df) {
  # Special dealing for customer GM and MOBIS module
  df <- df |>
    mutate(
      customer_pn_rev1 = case_when(
        고객명 == "한국지엠" ~ str_remove(customer_pn, "P"),
        고객명 == "MOBIS module" ~ temp,
        customer_pn == "392502" ~ "392502E000",
        TRUE ~ customer_pn
      ),
      .before = "customer_pn"
    )
  return(df)
}


process_inverter <- function(df) {
  # Special dealing for Inverter & others from MOBIS module
  df <- df |>
    mutate(
      customer_pn_rev = case_when(
        customer_pn_rev1 == "HIT100" ~ customer_pn,
        customer_pn_rev1 == "ME" ~ customer_pn,
        customer_pn_rev1 == "MV" ~ customer_pn,
        customer_pn_rev1 == "TSD" ~ customer_pn,
        customer_pn_rev1 == "cover" ~ customer_pn,
        customer_pn_rev1 == "NA" ~ customer_pn,
        customer_pn_rev1 == "V01" ~ customer_pn,
        customer_pn_rev1 == "V02" ~ customer_pn,
        customer_pn_rev1 == "V03" ~ customer_pn,
        customer_pn_rev1 == "366B02B" ~ customer_pn,
        TRUE ~ customer_pn_rev1
      ),
      .before = "customer_pn_rev1"
    )

  df <- df[, -which(names(df) == "customer_pn_rev1")]

  return(df)
}


summary_data <- function(df) {
  # Summary of data
  df <- df |>
    group_by(across(c(
      "plant", "고객명", "sold_to_party", "customer_name",
      "customer_pn_rev", "customer_pn", "material_number",
      "division", "profit_center", "material_description",
      "customer_material", "current_price", "cn_ty", "curr"
    ))) |>
    summarise(
      qty = sum(.data$billing_quantity),
      amt = sum(.data$sales_amount_krw)
    ) |>
    mutate(avg_billing_price = round(.data$amt / .data$qty, 2))
  return(df)
}


main <- function() {
  # Variables
  source(here(path, "src", "dplyr", "common_variable.R"))

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
  df_0180 <- read_excel(input_1) |>
    clean_names(ascii = FALSE) |>
    filter(!is.na(.data$sales_organization))

  df_2182 <- read_excel(input_2) |>
    clean_names(ascii = FALSE) |>
    filter(!is.na(.data$sales_organization))

  df_price <- read_tsv(input_3,
    skip = 3,
    locale = locale(encoding = "UTF-16LE"),
    col_types = cols(.default = col_character()),
  ) |>
    clean_names(ascii = FALSE) |>
    select(!c(1, "re_st")) |>
    rename(
      "curr" = "unit_9",
      "per_unit" = "unit_10",
      "current_price" = "amount"
    )

  df_customer <- read_csv(meta_1, show_col_types = FALSE) |>
    clean_names(ascii = FALSE) |>
    select(c("sold_to_party", "고객명"))

  df_customer_plant <- read_csv(meta_2,
    col_types = cols(.default = col_character()),
  ) |>
    clean_names(ascii = FALSE) |>
    select(c("ship_to_party", "공장명"))

  # Process data
  df_0180 <- df_0180 |> mutate(plant = "0180")
  df_2182 <- df_2182 |> mutate(plant = "2182")
  df <- bind_rows(df_0180, df_2182) |>
    relocate("plant", .after = "sales_organization")

  df_price <- df_price |> change_data_type()
  df_price_latest <- df_price |> get_latest_price()

  ## Join dataframes
  df <- df |>
    ##
    left_join(df_price_latest, by = c(
      "sold_to_party" = "customer",
      "material_number" = "material"
    )) |>
    ##
    left_join(df_customer, by = "sold_to_party") |>
    #  filter(!is.na(`고객명`)) |>
    relocate("고객명", .after = "plant") |>
    ##
    left_join(df_customer_plant, by = "ship_to_party") |>
    relocate("공장명", .after = "sold_to_party")

  df <- df |>
    process_mobis() |>
    process_customer_pn() |>
    extract_final_customer_pn() |>
    process_gm_mobis() |>
    process_inverter()

  df_summary <- df |> summary_data()

  # Write data
  write_excel_csv(df_summary, output_1)
  write_excel_csv(df_price_latest, output_2)
  write_excel_csv(df, output_3)
  print("Files are created")
}

main()
