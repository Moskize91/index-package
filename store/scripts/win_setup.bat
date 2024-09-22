@echo off

where python > nul 2>&1
if %errorlevel% neq 0 (
    echo please install python first.
    pause
    exit
)

cd ..\..\
python -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\pip.exe install -r requirements.txt
.venv\Scripts\python.exe -m spacy download en_core_web_sm
.venv\Scripts\python.exe -m spacy download zh_core_web_sm

pause