@REM Check R
where R

@REM CM 사급마감
Rscript src/dplyr/0_merge_copa_sales.R
Rscript src/dplyr/1_actual_sales.R

@REM 월마감
Rscript src/dplyr/2_budget_sales.R
Rscript src/dplyr/3_budget_std_costs.R

