import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from janitor import clean_names


# Path
path = "../"

# Read the data
df = pd.read_csv(path + "output/ZX05/CF costs.csv").clean_names()

# Change data type
df["costctr"] = df["costctr"].astype(str)

# Filter primary costs
primary_costs = [
    "299 Total Labor Costs",
    "465 Cost of materials",
    "535 Services In/Out",
]
df = df[df["acc_lv2"].isin(primary_costs)]

# Select columns
id_cols = [
    "fy",
    "period",
    "costctr",
    "gl_accounts",
    "profitctr",
    "fix_var",
    "department",
    "acc_lv2",
]
numeric_cols = [
    "plan",
    "actual",
    # "target",
    "delta_to_plan",
]
df = df[id_cols + numeric_cols]

# Change sign logic
df[numeric_cols] = df[numeric_cols] * -1e3
df["delta_to_plan"] = df["delta_to_plan"] * -1

# Summarize the data
df = df.groupby(id_cols).sum().reset_index()

top_10_negative = df.sort_values("delta_to_plan").head(10)
top_10_positive = df.sort_values("delta_to_plan").tail(10)


# Visualize the data
def delta_chart(df, y_col):
    df = df.groupby([y_col, "acc_lv2"]).sum().reset_index()
    plt = sns.barplot(
        data=df,
        x="delta_to_plan",
        y=y_col,
        hue="acc_lv2",
        palette=["#F2E500", "#4A4944", "#D7004B"],
    )
    plt.set(xlabel="Delta to Plan in M KRW", ylabel=y_col)
    plt.set(xscale="symlog")
    return plt


# Delta charts
p1 = delta_chart(df, "department")
p2 = delta_chart(df, "costctr")

p1
p2

# Data tables
print(top_10_negative)
print(top_10_positive)
print(df)
