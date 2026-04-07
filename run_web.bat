@echo off
setlocal
py -3 -m pip install -r requirements.txt
py -3 -m uvicorn app_web:app --host 0.0.0.0 --port 8000
