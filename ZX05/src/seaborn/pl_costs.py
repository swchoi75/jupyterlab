import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
from janitor import clean_names


# Path
path = Path(__file__).parent
output_path = path.parent.parent / "output"

# Read the data
df = pd.read_csv(output_path / "PL fix costs.csv").clean_names()

# Change data type
df["costctr"] = df["costctr"].astype(str)

# Filter primary costs
primary_costs = ["PE production", "PE materials management", "PE plant administration"]
df = df[df["race_item"].isin(primary_costs)]

# Select columns
id_cols = [
    "costctr",
    "gl_accounts",
    "profit_center",
    "fix_var",
    "department",
    "acc_lv2",
    "bu",
    "division",
    "plant_name",
    "outlet_name",
    "ce_text",
    "race_item",
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
    df = df.groupby([y_col, "division"]).sum().reset_index()
    plt = sns.barplot(
        data=df,
        x="delta_to_plan",
        y=y_col,
        hue="division",
        palette=["#64AF59", "#006EAA"],
    )
    plt.set(xlabel="Delta to Plan in M KRW", ylabel=y_col)
    plt.set(xscale="symlog")
    return plt


# Delta charts
p1 = delta_chart(df, "outlet_name")
p2 = delta_chart(df, "ce_text")

p1
p2

# Data tables
print(top_10_negative)
print(top_10_positive)
print(df)
