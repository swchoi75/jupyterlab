import pandas as pd
from pathlib import Path
from janitor import clean_names


# Path
try:
    path = Path(__file__).parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent


# Responsibilities
outlet_cf = ["Central Functions"]
outlet_pl1 = ["PL ENC", "PL DTC", "PL MTC", "PL VBC"]
outlet_pl2 = ["PL CM CCN", "PL CM CVS", "PL HVD", "PL MES", "PL ETH"]
outlet_rnd = ["DIV E Eng.", "PL MES"]
outlet_shs = ["DIV E C", "DIV P C", "PT - Quality"]


# Filenames
input_file = path / "fc_output" / "fc_monthly_spending.csv"

output_file_cf = path / "fc_output" / "fc_monthly_spending_cf.csv"
output_file_pl1 = path / "fc_output" / "fc_monthly_spending_pl1.csv"
output_file_pl2 = path / "fc_output" / "fc_monthly_spending_pl2.csv"
output_file_rnd = path / "fc_output" / "fc_monthly_spending_rnd.csv"
output_file_shs = path / "fc_output" / "fc_monthly_spending_shs.csv"


# Read data
df = pd.read_csv(input_file)


# Filter data
df_cf = df[df["outlet_sender"].isin(outlet_cf)]
df_pl1 = df[
    (df["location_receiver"] == "Icheon") & (df["outlet_sender"].isin(outlet_pl1))
]
df_pl2 = df[
    (df["location_receiver"] == "Icheon") & (df["outlet_sender"].isin(outlet_pl2))
]
df_rnd = df[
    (df["location_receiver"] == "Icheon NPF") & (df["outlet_sender"].isin(outlet_rnd))
]
df_shs = df[
    (df["location_receiver"] == "Icheon NPF") & (df["outlet_sender"].isin(outlet_shs))
]


# Write data
df_cf.to_csv(output_file_cf, index=False)
df_pl1.to_csv(output_file_pl1, index=False)
df_pl2.to_csv(output_file_pl2, index=False)
df_rnd.to_csv(output_file_rnd, index=False)
df_shs.to_csv(output_file_shs, index=False)
print("Files are created")
