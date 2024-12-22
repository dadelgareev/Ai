@echo off
REM Путь к виртуальному окружению и скриптам
set VENV_PATH=C:\Users\Adel\Ai_Recommendation\Scrapper\ScraperLamoda\.venv
set SCRIPT_PATH=C:\Users\Adel\Ai_Recommendation\Scrapper\ScraperLamoda

REM Активируем виртуальное окружение
call "%VENV_PATH%\Scripts\activate.bat"

REM Запускаем serverAI.py и сохраняем код возврата
python "%SCRIPT_PATH%\serverAI.py"
set EXIT_CODE=%ERRORLEVEL%

REM Проверяем код возврата
if %EXIT_CODE% NEQ 0 (
    echo Скрипт завершился с ошибкой. Код ошибки: %EXIT_CODE%
    pause
) else (
    echo Скрипт выполнен успешно.
)

REM Деактивируем виртуальное окружение
call deactivate
