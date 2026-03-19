@echo off
echo Installing required Python libraries...
echo.

python -m pip install --upgrade pip

pip install requests
pip install beautifulsoup4
pip install aiohttp
pip install python-telegram-bot==20.7
pip install selenium

echo.
echo ✅ All required libraries installed.
pause
