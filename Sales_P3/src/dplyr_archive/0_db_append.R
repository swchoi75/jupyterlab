source("script/db_functions.R")


# Input data
filename <- "data/Actual/COPA_2023_06.TXT" # Update monthly


# Process data
df <- db_append(filename)


# Output data
write_csv(df, "db/COPA_2023.csv",
    append = TRUE
)
