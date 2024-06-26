import pandas as pd
import sidetable
import seaborn as sns

df = sns.load_dataset("titanic")

df["class"].value_counts

# freq
df.stb.freq(["class"], style=True)

# missing
df.stb.missing()

# subtotal
df.groupby(["sex", "class"]).agg({"fare": ["mean"]}).stb.subtotal()

df.groupby(["embark_town", "class", "sex"]).agg(
    {"fare": ["mean"], "age": ["mean"]}
).unstack().stb.flatten()

# pretty
df.groupby(["pclass", "sex"]).agg({"fare": "sum"}).div(df["fare"].sum()).stb.pretty()

df.groupby(["pclass", "sex"]).agg({"fare": "sum"}).div(df["fare"].sum()).stb.pretty(
    precision=0, caption="Fare Percentage"
)

# bin
df.stb.freq(["fare"])

df["fare_bin"] = pd.qcut(df["fare"], q=4, labels=["low", "medium", "high", "x-high"])
df.stb.freq(["fare_bin"])

df.stb.freq(["deck"])

df["deck_fillna"] = df["deck"].cat.add_categories("UNK").fillna("UNK")
df.stb.freq(["deck_fillna"])


df.stb.freq(["deck", "class"])
df.stb.freq(["deck", "class"], clip_0=False)
