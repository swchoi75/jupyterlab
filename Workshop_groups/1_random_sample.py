import random
import pandas as pd
from pathlib import Path


# Path
path = Path(__file__).parent


# Functions
def main():
    # Filenames
    input_file = path / "5_30일 (목) 워크숍 장소 _ activity 선호도 조사(1-23).xlsx"
    output_file = path / "workshop_groups.csv"

    # Read data
    df = pd.read_excel(input_file)

    # Process data

    ## Lists
    list_of_names = df["Name"].to_list()
    random_list = random.sample(list_of_names, len(list_of_names))
    list_of_groups = ["A"] * 6 + ["B"] * 6 + ["C"] * 6 + ["D"] * 5

    ## Dataframes
    df_1 = pd.DataFrame(list_of_groups)
    df_2 = pd.DataFrame(random_list)
    result = pd.concat([df_1, df_2], axis="columns")
    result.columns = ["Groups", "Names"]

    # Write data
    result.to_csv(output_file, index=False)
    print("A file is created.")


if __name__ == "__main__":
    main()
