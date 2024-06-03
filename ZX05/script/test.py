import pandas as pd

# Create a DataFrame with a column containing the date strings
df = pd.DataFrame({"Date": ["PL_2024_02", "PL_2024_03", "PL_2024_04"]})

# Convert the date strings to datetime format
df["Date"] = df["Date"].str.extract(r"([0-9]{4}_[0-9]{2})")
df["Date"] = pd.to_datetime(df["Date"], format="%Y_%m")

# Extract the year and month
df["Year"] = df["Date"].dt.year
df["Month"] = df["Date"].dt.month
