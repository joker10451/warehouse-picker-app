@echo off
setlocal
py -3 -m pip install -r requirements.txt
py -3 -m PyInstaller --noconfirm --onefile --windowed --name warehouse_app main.py
echo Build completed. EXE is in dist\warehouse_app.exe
pause
