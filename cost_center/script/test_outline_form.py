import pandas as pd

# Example DataFrame
df = pd.DataFrame(
    {
        "Category": ["A", "A", "B", "B", "C", "C"],
        "Subcategory": ["X", "Y", "X", "Y", "X", "Y"],
        "Values": [10, 20, 30, 40, 50, 60],
    }
)

# Group by Category and Subcategory, summing the Values
grouped_df = df.groupby(["Category", "Subcategory"])["Values"].sum().reset_index()

# Create subtotals for each Category
subtotal_category = grouped_df.groupby("Category")["Values"].sum().reset_index()
subtotal_category["Subcategory"] = "Subtotal"

# Concatenate the original grouped DataFrame with the subtotals
result_df = pd.concat([grouped_df, subtotal_category], ignore_index=True)

# Sort the DataFrame to maintain the order
result_df = result_df.sort_values(
    by=["Category", "Subcategory"], ascending=[True, False]
).reset_index(drop=True)

# Format the DataFrame to have an outline form
result_df["Outline"] = result_df.apply(
    lambda x: (
        x["Category"] if x["Subcategory"] == "Subtotal" else "    " + x["Subcategory"]
    ),
    axis=1,
)
result_df = result_df[["Outline", "Values"]]

print(result_df)
