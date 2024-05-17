@REM Check python
where python

@REM BOM
python script/1-0_representative_pn_2024.py
python script/1-1_bom_csv_to_parquet.py
python script/1-2_merge_bom.py
python script/1-3_bom_price.py

@REM FX Rate
python script/2-1_fx_rates_ytd.py
python script/2-2_fx_rates_spot.py
python script/2-3_merge_fx_rates.py
python script/2-4_fx_rates_HMG.py

@REM FX scenarios with 8 options
python script/3-1_bom_price_fx_rates.py 1
python script/3-1_bom_price_fx_rates.py 2
python script/3-1_bom_price_fx_rates.py 3
python script/3-1_bom_price_fx_rates.py 4
python script/3-1_bom_price_fx_rates.py 5
python script/3-1_bom_price_fx_rates.py 6
python script/3-1_bom_price_fx_rates.py 7
python script/3-1_bom_price_fx_rates.py 8

@REM FX impact
python script/3-2_sales_bom_price_delta.py

@REM Power BI
python script/4-1_FX_compensation.py
python script/4-2_Power_BI.py

@REM Test script
python script/5-1_test_missing_meta.py
