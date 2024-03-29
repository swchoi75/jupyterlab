@REM Check python
where python

@REM Preprocessing
python script/0_derive_cc_and_prj.py

@REM GPA spending
python script/1_fc_GPA_spending.py

@REM Acquisition
python script/2_fc_acquisition.py
python script/2_act_SAP_acquisition.py

@REM 
python script/3_adjustment_manually.py
python script/3_export_additional_data.py

@REM Depreciation
python script/4_fc_depreciation.py
python script/4_act_depreciation.py
python script/4_auc_depreciation.py
python script/4_merge_versions.py

@REM Central Function Allocation
python script/5_central_function_allocation.py
python script/5_reorder_columns.py