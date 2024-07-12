
Write-Output "============================"
Write-Output "Activate Virtual Environment"
Write-Output "============================"

C:\Users\uid98421\.virtualenvs\py39\Scripts\activate.ps1

Write-Output "============================"
Write-Output "Run Python Scripts"
Write-Output "============================"


Write-Output "1. Process CSV file"
python "C:\Users\uid98421\OneDrive - Vitesco Technologies\GitHub\jupyterlab\Jour_fixe\script\process_csv.py"

Write-Output "Please Wait..."
Start-Sleep -Seconds 5

Write-Output "2. Produce Excel file"
python "C:\Users\uid98421\OneDrive - Vitesco Technologies\GitHub\jupyterlab\Jour_fixe\script\produce_excel.py"


Write-Output "============================"
Write-Output "End"
Write-Output "============================"

