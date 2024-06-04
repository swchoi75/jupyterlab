@REM Check python
where python

@REM Running the scripts for Actual
python script/0_create_db.py
python script/1_actual_central_function.py
python script/1_actual_productline_fix.py
python script/1_actual_productline_var_FGK_P.py
python script/1_actual_productline_var_SCM.py
@REM python script/1_optional_actual_overtime.py

