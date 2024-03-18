@REM Check python
where python

@REM Preprocessing
python src/0_derive_cc_and_prj.py

@REM GPA spending
python src/1_fc_GPA_spending.py

@REM Acquisition
python src/2_fc_acquisition.py
python src/2_act_SAP_acquisition.py

@REM 
python src/3_adjustment_manually.py
python src/3_export_additional_data.py

@REM Depreciation
python src/4_fc_depreciation.py
python src/4_act_depreciation.py
python src/4_auc_depreciation.py
python src/4_merge_versions.py

@REM Central Function Allocation
python src/5_central_function_allocation.py
python src/5_reorder_columns.py