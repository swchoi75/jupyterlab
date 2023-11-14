#!/bin/bash
echo "Please Wait..."
echo "============================"
echo "Python script.py"
echo "============================"


/opt/miniconda3/bin/python /home/uid98421/Jour_fixe/SP_process_csv.py

sleep 5

/opt/miniconda3/bin/python /home/uid98421/Jour_fixe/SP_produce_excel.py

echo "============================"
echo "End"
echo "============================"
