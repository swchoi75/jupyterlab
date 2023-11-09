import pandas as pd
import plotly
import plotly.express as px
from pathlib import Path
from janitor import clean_names


# import plotly.figure_factory as ff

EXCEL_FILE = Path.cwd() / "Tasks.xlsx"

# Read Dataframe from Excel file
df = pd.read_excel(EXCEL_FILE).clean_names()
df = df.rename(columns={"complete_in_%": "progress"})


# Create Gantt Chart
fig = px.timeline(
    df,
    x_start=df.start,
    x_end=df.finish,
    y=df.task,
    color=df.progress,
    title="Task Overview",
)

# Upade/Change Layout
fig.update_yaxes(autorange="reversed")
fig.update_layout(title_font_size=42, font_size=18, title_font_family="Arial")

# Interactive Gantt
# fig = ff.create_gantt(df)

# Save Graph and Export to HTML
plotly.offline.plot(fig, filename="Task_Overview_Gantt.html")
