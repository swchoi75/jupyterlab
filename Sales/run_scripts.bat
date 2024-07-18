@REM Check python
where python

@REM Sales
python script/0_merge_copa_sales.py
python script/1_sales_overview.py

@REM Sales for Division Electrification
python script/2_sales_Div_E.py
python script/3_sales_Div_E_with_meta.py
python script/4_sales_Div_E_high_runner.py

@REM Testing
python script/5_test_missing_meta.py
python script/6_test_representative_coverage.py


