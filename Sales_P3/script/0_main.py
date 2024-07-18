import pandas as pd
from pathlib import Path


from actualsales import process_actual_data, process_std_costs
from budgetsales import process_budget_data
from budgetstdcosts import budget_std_costs
from copasales import process_copa_sales, sales_ytd
from missingmaster import (
    missing_customer_center,
    missing_gl_accounts,
    missing_customer_material,
    missing_material_master,
    missing_product_hierarchy,
)
from pricebudget import (
    bud_price_div_e,
    bud_price_div_p,
    bud_price_pl_cm,
    df_div_e,
    df_div_p,
    df_pl_cm,
    spv_mapping,
)
from priceimpact import calculate_price_impact, replace_missing_values
from pricevolmix import join_with_cm_ratio, delta_impact


# Path
path = Path(__file__).parent.parent


def main():

    # Filenames
    copa_path = path / "db" / "COPA_Sales_2024.parquet"
    kappa_path = "data/Actual/Kappa HEV adj_costs.xlsx"

    # Process actual data
    df_act = process_actual_data(copa_path)

    kappa_cost = process_std_costs(kappa_path)

    df_act = pd.concat([df_act, kappa_cost], ignore_index=True)

    # Process budget data
    budget_path = (
        "data/Plan/2023_BPR_Consolidation_20220907_CMU Material EQ update.xlsx"
    )
    df_bud = process_budget_data(budget_path)

    df_bud = budget_std_costs(df_bud)

    # Bind actual and budget data and write to a file
    df = pd.concat([df_act.assign(Version="Actual"), df_bud.assign(Version="Budget")])
    cols = [
        "Version",
        "Year",
        "Month",
        # "Period",
        "Profit Ctr",
        "RecordType",
        "Cost Elem.",
        "Account Class",
        "Plnt",
        "Product",
        "Material Description",
        "Sold-to party",
        "Sold-to Name 1",
        "Qty",
        "Sales_LC",
        "STD_Costs",
    ]
    df = df[cols]  # reorder columns
    df.to_csv("output/COPA Sales and Budget.csv", index=False)

    # Add additional information
    df = process_copa_sales(df)

    # Check missing master data and write to files
    missing_customer_center(df).to_csv("meta/missing CC_2023.csv", index=False)
    missing_gl_accounts(df).to_csv("meta/missing GL.csv", index=False)
    missing_customer_material(df).to_csv(
        "meta/missing Customer Material.csv", index=False
    )

    missing_material_master(df).query("`Profit Ctr` != '50802-018'").to_excel(
        "meta/missing Material_0180.xlsx", index=False, header=False
    )
    missing_material_master(df).query("`Profit Ctr` == '50802-018'").to_excel(
        "meta/missing Material_2182.xlsx", index=False, header=False
    )

    missing_product_hierarchy(df).to_csv("meta/missing PH.csv", index=False)

    # Sales P3: 3 different budget price table
    bud_price_div_e(df).to_csv("output/bud_price_div_e.csv", index=False)
    bud_price_div_p(df).to_csv("output/bud_price_div_p.csv", index=False)
    bud_price_pl_cm(df).to_csv("output/bud_price_pl_cm.csv", index=False)

    # Sales P3: YTD Sales
    df_ytd = sales_ytd(df)
    df_ytd.to_csv("output/YTD Sales.csv", index=False)

    # Sales P3: SPV mapping for 3 different scenarios
    df_ytd_act = df_ytd[df_ytd["Version"] == "Actual"]
    df_ytd_bud = df_ytd[df_ytd["Version"] == "Budget"]

    map_div_e = spv_mapping(
        df_div_e(df_ytd_act), bud_price_div_e(df), "Customer Material"
    )
    map_div_p = spv_mapping(df_div_p(df_ytd_act), bud_price_div_p(df), "Product")
    map_pl_cm = spv_mapping(df_pl_cm(df_ytd_act), bud_price_pl_cm(df), "CM Cluster")

    df = pd.concat([map_div_e, map_div_p, map_pl_cm, df_ytd_bud])

    # Sales P3: Price, Volume, Mix
    df = calculate_price_impact(df)
    df = replace_missing_values(df)

    df = join_with_cm_ratio(df)
    df = delta_impact(df)

    df.to_csv("output/YTD Sales_P3.csv", index=False)
    print("Files are created")


if __name__ == "__main__":
    main()
