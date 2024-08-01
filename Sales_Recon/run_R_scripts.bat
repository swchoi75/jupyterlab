@REM Check R
where R

@REM Actual sales
Rscript src/dplyr/0_merge_copa_sales.R
Rscript src/dplyr/1_actual_sales.R

@REM Budget sales
Rscript src/dplyr/2_budget_sales.R
Rscript src/dplyr/3_budget_std_costs.R

@REM Combine Actual and Budget Sales & add master data
Rscript src/dplyr/4_concat_actual_budget.R
Rscript src/dplyr/5_add_master_data.R

@REM Sales P3 analysis
Rscript src/dplyr/6_spv_mapping.R
Rscript src/dplyr/7_price_impact.R
Rscript src/dplyr/8_price_vol_mix.R

@REM check completeness of master data
Rscript src/dplyr/9_test_missing_master.R

