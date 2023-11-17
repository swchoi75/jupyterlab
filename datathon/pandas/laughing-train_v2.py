import pandas as pd


# Path
path = "../"
data_path = path + "datathon/"
output_path = path + "datathon/"

# Input data
pricing = pd.read_csv(data_path + "pricing.csv")


def department():
    df = pd.read_csv(data_path + "department.csv")
    df["department"] = df["department"].str.strip()
    return df


def main_data():
    df = pd.read_csv(data_path + "metrics.csv")
    df = process_date_columns(df)
    df = pd.merge(df, department(), how="left", on="id")
    return df


def process_date_columns(df):
    df["data_timestamp"] = pd.to_datetime(df["data_timestamp"])
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    df["last_patch"] = pd.to_datetime(df["last_patch"])
    return df


def text_columns(df):
    df = df[["id", "department", "disk_size", "type", "size"]]
    df = df.drop_duplicates()
    return df


df = main_data()
df_text = text_columns(df)


# 1.2 How many departments use the appliances of the Data Platform?
def number_of_department(df):
    df = df[["id", "department"]]
    df = df.groupby("department").count()
    df = df.sort_values("id", ascending=False)
    return df


number_of_department(df_text)
len(number_of_department(df_text))


# 1.3 What is the most popular appliance size used by all departments?
# And how many of those popular sizes did you find in the whole dataset?
def appliance_size(df):
    df = df[["id", "size"]]
    df = df.groupby("size").count()
    df = df.sort_values("id", ascending=False)
    return df


appliance_size(df_text)


# 2.1 Which is the most popular appliance type per department?
def type_by_dept(df):
    df = df[["id", "department", "type"]]
    df = df.groupby(["department", "type"]).count()
    df = df.sort_values(["department", "id"], ascending=[True, False])
    return df


type_by_dept(df_text)


# 2.2 Wich appliance size had the lowest vCPU utilization
# over the full time range of the dataset based on the listed metrics?
# Calculate a value with 6 digits after zero for each metric:
def vcpu_by_size(df):
    df = df[["id", "data_timestamp", "size", "vcpu"]]
    df = df.drop_duplicates()
    df = df[["size", "vcpu"]]
    df = df.groupby("size", as_index=False).agg(["min", "median", "mean"])
    return df


vcpu_by_size(df)
vcpu_by_size(df)["vcpu", "min"].sort_values()
vcpu_by_size(df)["vcpu", "median"].sort_values()
vcpu_by_size(df)["vcpu", "mean"].sort_values()


# 2.3 Which department has used the most appliances
#     between 15.12.2022 and 16.01.2023?
#     How many appliances did they use in this time range?


def data_in_periods(df):
    mask = (df["data_timestamp"] > pd.to_datetime("2022-12-15")) & (
        df["data_timestamp"] < pd.to_datetime("2023-01-16")
    )
    df = df.loc[mask]
    return df


def appliance_in_periods(df):
    df = data_in_periods(df)
    df = df[["id", "department"]]
    df = df.drop_duplicates()
    df = df.groupby("department").count()
    df = df.sort_values("id", ascending=False)
    return df


appliance_in_periods(df)


# 2.4 What is the most expensive size of an appliance used
# in the Data Platform in terms of hours used per department?
def data_usage(df):
    df = pd.merge(df, pricing, how="left", on="size")
    df = df[["department", "size", "data_timestamp", "cost_per_hour"]]
    df = df.drop_duplicates()
    df = df.groupby(["department", "size", "cost_per_hour"],
                    as_index=False).count()
    return df


def calc_cost(df):
    df = data_usage(df)
    df["cost"] = df["data_timestamp"] / 12 * df["cost_per_hour"]
    df = df.sort_values(["department", "cost"], ascending=False)
    return df


calc_cost(df)


# 3.1 Which fields are important to find out if an appliance is idle
# - meaning that an appliance is running but no action is performed on it?
# Sort the correct values in alphabetic order, before submitting your response.


# 3.2 Which appliances were idle and when?
def check_idle(df):
    df = pd.merge(df, maximum_network(df), how="left", on="size")
    df["network_idle"] = df.apply(check_network, axis=1)
    df["vcpu_idle"] = df.apply(check_vcpu, axis=1)
    df["idle"] = df.apply(both_idle, axis=1)
    return df


def both_idle(row):
    if row["vcpu_idle"] == "idle" and row["network_idle"] == "idle":
        return "yes"
    else:
        return "no"


def check_vcpu(row):
    if row["type"] == "deeplearning" and row["vcpu"] < 10 or row["vcpu"] < 5:
        return "idle"
    else:
        return ""


def check_network(row):
    if row["net_in"] + row["net_out"] < row["net_max"] * 0.02:
        return "idle"
    else:
        return ""


def maximum_network(df):
    df = df[["size", "net_in", "net_out"]]
    df = df.groupby("size", as_index=False).agg("max")
    df["net_max"] = df["net_in"] + df["net_out"]
    df = df[["size", "net_max"]]
    return df


def export_idle_verbose(df):
    df = check_idle(df)
    df = df[
        [
            "id",
            "data_timestamp",
            "idle",
            "vcpu_idle",
            "vcpu",
            "network_idle",
            "net_in",
            "net_out",
            "net_max",
        ]
    ]
    df = df.drop_duplicates()
    df = df.sort_values(["data_timestamp", "id"], ascending=False)
    return df


def export_idle_final(df):
    df = export_idle_verbose(df)
    df = df[["id", "data_timestamp", "idle"]]
    return df


export_idle_verbose(df).to_csv(output_path + "idle_verbose.csv", index=False)
export_idle_final(df).to_csv(output_path + "idle_final.csv", index=False)


# 3.3.1 How much costs did the appliances generate in the idle state?


# 3.3.2 Compared to the total cost generated overall,
# how much percent are attributed to the idle appliances?
