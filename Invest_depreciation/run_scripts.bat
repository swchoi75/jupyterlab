@REM Check python
where python

@REM Cost center information
python script/0_derive_cost_centers.py

@REM GPA
python script/1_fc_GPA_spending.py
python script/2_fc_acquisition.py
python script/3_fc_depreciation.py

@REM SAP
python script/2_act_SAP_acquisition.py
python script/3_act_depreciation.py
python script/3_auc_depreciation.py

@REM GPA + SAP
python script/4_combine_versions.py
python script/4_export_additional_data.py

@REM Central Function Allocation
python script/5_central_function_allocation.py