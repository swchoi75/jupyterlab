
import seaborn as sns
from bokeh.plotting import figure, output_file, save

# Load data
tips = sns.load_dataset("tips")

# Create plot
sns.scatterplot(x="total_bill", y="tip", hue="sex", data=tips)

# Convert plot to Bokeh object
bokeh_fig = figure(title="Seaborn Plot", x_axis_label="Total Bill", y_axis_label="Tip")
bokeh_fig.scatter(x=tips["total_bill"], y=tips["tip"], color=tips["sex"])

# Export Bokeh object to JSON file
output_file("seaborn_plot.json")
save(bokeh_fig)
