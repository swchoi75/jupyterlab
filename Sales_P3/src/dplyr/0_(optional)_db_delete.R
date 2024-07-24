source("script/db_functions.R")


# Input data
db_file <- "db/COPA_2023.csv"


# Process data
df <- db_delete(db_file)


# Output data
write_csv(df, db_file)
