@echo off
title R-Trader Pro Database Manager
color 0A

echo ========================================
echo   R-TRADER PRO DATABASE SYSTEM
echo   Version 1.0 - For Traders
echo ========================================
echo.

:MENU
echo.
echo 1. Start System (Khoi dong he thong)
echo 2. View Data (Xem du lieu)
echo 3. Stop System (Dung he thong)
echo 4. View Logs (Xem logs)
echo 5. Backup Database
echo 6. Export Today Data
echo 7. Exit
echo.

set /p choice="Chon chuc nang (1-7): "

if "%choice%"=="1" goto START
if "%choice%"=="2" goto VIEW_DATA
if "%choice%"=="3" goto STOP
if "%choice%"=="4" goto LOGS
if "%choice%"=="5" goto BACKUP
if "%choice%"=="6" goto EXPORT
if "%choice%"=="7" goto EXIT

echo Lua chon khong hop le!
goto MENU

:START
echo.
echo [+] Dang khoi dong Docker Desktop...
start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"
timeout /t 30

echo [+] Dang khoi dong Database System...
cd /d "D:\Code LLM\GetDB Level2\MakeDB"
docker-compose up -d

echo.
echo [✓] He thong da khoi dong!
echo [i] R-Trader Pro port: 3012
echo [i] Database port: 5432
echo.
pause
goto MENU

:VIEW_DATA
echo.
echo [+] Dang mo Database Manager...
cd /d "D:\Code LLM\GetDB Level2\MakeDB"
python db_manager.py
pause
goto MENU

:STOP
echo.
echo [+] Dang dung he thong...
cd /d "D:\Code LLM\GetDB Level2\MakeDB"
docker-compose down
echo [✓] Da dung he thong!
pause
goto MENU

:LOGS
echo.
echo [+] Dang xem logs (Nhan Ctrl+C de thoat)...
cd /d "D:\Code LLM\GetDB Level2\MakeDB"
docker-compose logs -f --tail=50
pause
goto MENU

:BACKUP
echo.
echo [+] Dang backup database...
cd /d "D:\Code LLM\GetDB Level2\MakeDB"
set backup_name=backup_%date:~10,4%%date:~4,2%%date:~7,2%_%time:~0,2%%time:~3,2%
set backup_name=%backup_name: =0%
mkdir backups 2>nul
docker-compose exec -T db pg_dump -U postgres rithmic_db > backups\%backup_name%.sql
echo [✓] Backup xong: backups\%backup_name%.sql
pause
goto MENU

:EXPORT
echo.
echo [+] Dang export du lieu hom nay...
cd /d "D:\Code LLM\GetDB Level2\MakeDB"
python -c "import db_manager; m=db_manager.DatabaseManager(); m.connect(); m.export_to_csv()"
echo [✓] Export xong vao thu muc exports!
pause
goto MENU

:EXIT
echo.
echo Cam on da su dung!
pause
exit