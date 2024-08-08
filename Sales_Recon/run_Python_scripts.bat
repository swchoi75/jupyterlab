@REM Check python
where python

@REM CM 사급마감
python src/pandas/0_CM_사급마감_(정마감).py
python src/pandas/0_CM_사급마감_PIR.py

@REM 월마감
python src/pandas/1-1_VAN_Tax_invoice.py
python src/pandas/1-2_SAP_billing_report.py
python src/pandas/1-2-1_List_of_price_download.py

python src/pandas/2-1_Join_Customer_PN.py
python src/pandas/2-2_Join_Tax_invoice_and_SAP_billing.py
python src/pandas/2-3_Join_Material.py

python src/pandas/3_merge_입출고비교.py

python src/pandas/4_BU_price_difference.py

python src/pandas/5_upload_batch_file.py

python src/pandas/6-1_Tax_invoice_in_SAP.py
python src/pandas/6-2_SAP_uninvoiced_sales.py


