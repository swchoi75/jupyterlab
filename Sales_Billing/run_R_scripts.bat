@REM Check R
where R

@REM Compare KE30 and GL stock value
Rscript src/dplyr/merge_KE30_sales.R
Rscript src/dplyr/merge_GL_stock_value.R
Rscript src/dplyr/report_KE30_and_GL.R


