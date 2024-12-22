@echo off
REM Путь к виртуальному окружению и скриптам
set VENV_PATH=D:\Python\Data_parser\Ai_App_Recommendation\3ServicesWith\ParserWithProtos\.venv
set SCRIPT_PATH=D:\Python\Data_parser\Ai_App_Recommendation\3ServicesWith\ParserWithProtos

REM Активируем виртуальное окружение и запускаем serverAI.py
call "%VENV_PATH%\Scripts\activate.bat"
python "%SCRIPT_PATH%\ImageEmbeddingClient.py"

