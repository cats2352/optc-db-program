@echo off

:: [해결] 한글 깨짐(인코딩) 문제를 방지하기 위해 코드 페이지를 UTF-8(65001)로 변경합니다.
chcp 65001 > nul

:: 배치 파일이 있는 폴더로 이동합니다.
cd /d "%~dp0"

echo [INFO] 파이썬 스크립트를 실행합니다...
python va.py

echo.
echo 작업이 완료되었습니다.
pause