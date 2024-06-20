# README

## Create and Open Jupyterlab

1. Go to Datalake at https://datalake.vitesco.io/

2. Login with uid and Windows password

3. Go to "Appliances" tab

4. Click "New Appliance" button

5. Select "Jupyter" and Click "Continue"

6. Input instancename, Select CPU cores 2, Memory 4 GB, Disksize 30 GB, and Click "Create"

7. Wait for two minutes

8. When green light is on, open "Jupyterlab" and login with your credentials as above.


## Data upload from Local to Datalake

1. Go to Datalake at https://datalake.vitesco.io/

2. Go to "Datasets" tab

3. Click "Generate Access Keys" Button on the upper right side

4. Select PowerShell tab

5. Click "Copy Commands to Clipblard"

6. Open "Windows PowerShell" by pressing Windows key + "S"

7. Paset in the commmands copied from above by "Ctrl + V"

8. Paste in the following commands by "Ctrl + C" and "Ctrl + V"

# Data upload
aws s3 sync --dryrun C:/Users/uiv17345/"Vitesco Technologies"/"Controlling VT Korea - Documents"/"120. Data automation/jupyterlab"/PnL/ s3://datalake-eu-central-1-user-vt-prod/qAuu-uiv17345/jupyterlab/PnL/


## In the Jupyterlab ###

1. Go to Web browser for JupyterLab

2. Open terminal and input the following command

    > pip install pyjanitor
    > cp -a ~/datasets/home-dataset/jupyterlab/PnL/. ~/PnL/

3. In case of the Error " No module named 'commonfunctions'", Manually overwrite the files in the "script" folder

4. Double click to open "1_run_scripts_Actual.ipynb" file

5. Click 4th tab on the left side panel to see "Table of Contents"

6. Click ">>" Button for "Run All Cells"


## Closing Jupyterlab on Datalake

[Important] Stop or Terminate Jupyterlab to avoid costs !