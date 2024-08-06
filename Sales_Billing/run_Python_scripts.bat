@REM Check Python
where python

@REM Compare KE30 and GL stock value
python src/pandas/merge_KE30_sales.py
python src/pandas/merge_GL_stock_value.py
python src/pandas/report_KE30_and_GL.py


