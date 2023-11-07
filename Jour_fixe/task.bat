@ECHO OFF 
TITLE Execute python script on anaconda environment
ECHO Please Wait...

:: Section 1: Activate the environment.
ECHO ============================
ECHO Conda Activate
ECHO ============================
@CALL activate "C:\bin\miniconda3\envs\py39"

:: Section 2: Execute python script.
ECHO ============================
ECHO Python script.py
ECHO ============================
python "C:\Users\uid98421\OneDrive - Vitesco Technologies\FY 2023\Jour fixe\process_excel - jour_fixe.py"

ECHO ============================
ECHO End
ECHO ============================

@call conda deactivate

PAUSE
