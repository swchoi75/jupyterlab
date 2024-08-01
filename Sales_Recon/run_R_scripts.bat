@REM Check R
where R

@REM CM 사급마감
Rscript src/dplyr/0_CM_사급마감_(정마감).R
Rscript src/dplyr/0_CM_사급마감_PIR.R

@REM 월마감
Rscript src/dplyr/1-1_VAN_Tax_invoice.R
Rscript src/dplyr/1-2_SAP_billing_report.R
Rscript src/dplyr/1-2-1_List_of_price_download.R

Rscript src/dplyr/2-1_Join_Customer_PN.R
Rscript src/dplyr/2-2_Join_Tax_invoice_and_SAP_billing.R
Rscript src/dplyr/2-3_Join_Material.R

Rscript src/dplyr/3_merge_입출고비교.R

Rscript src/dplyr/4_BU_price_difference.R

Rscript src/dplyr/5_upload_batch_file.R

Rscript src/dplyr/6-1_Tax_invoice_in_SAP.R
Rscript src/dplyr/6-2_SAP_uninvoiced_sales.R


