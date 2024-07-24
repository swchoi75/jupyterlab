@REM Check python
where python

@REM Actual sales
python src/pandas/0_merge_copa_sales.py
python src/pandas/1_actual_sales.py

@REM Budget sales
python src/pandas/2_budget_sales.py
python src/pandas/3_budget_std_costs.py

@REM Combine Actual and Budget Sales & add master data
python src/pandas/4_concat_actual_budget.py
python src/pandas/5_add_master_data.py

@REM Sales P3 analysis
python src/pandas/6_spv_mapping.py
python src/pandas/7_price_impact.py
python src/pandas/8_price_vol_mix.py

@REM check completeness of master data
python src/pandas/9_test_missing_master.py

