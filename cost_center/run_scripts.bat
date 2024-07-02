@REM Check python
where python

@REM Running the scripts

python script/1_primary_cc_report.py
python script/2_report_fix_actual_to_plan.py

python script/3-0_report_summary.py
python script/3-1_report_in_sidetable.py
python script/3-2_further_refine_report.py

python script/4_report_in_excel.py


