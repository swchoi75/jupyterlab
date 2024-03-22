@REM Check python
where python

@REM BOM
python script/1-1_bom_csv_to_parquet.py
python script/1-2_merge_bom.py
python script/1-3_bom_price.py

@REM FX Rate
python script/2-1_fx_rates_ytd.py
python script/2-2_fx_rates_spot.py
python script/2-3_merge_fx_rates.py
python script/2-4_fx_rates_HMG.py

@REM FX scenarios with manual options
python script/3-1_bom_price_fx_rates.py

@REM Manual Run is necessary for Script 3-1
