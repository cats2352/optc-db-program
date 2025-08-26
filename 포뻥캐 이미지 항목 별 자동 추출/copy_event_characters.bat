@echo off

REM 스크립트가 있는 폴더로 경로를 이동합니다.
REM 경로에 공백이 있으므로 큰따옴표("")로 감싸줍니다.
cd /d "C:\Users\Administrator\Desktop\db제작본보관\포뻥캐 자동 추출"

REM Python 스크립트를 실행합니다.
echo 파이썬 스크립트를 실행합니다...
REM 아래와 같이 파일 이름 앞에 'python'을 추가해야 합니다.
python convert_serverid_to_logbookid.py

echo.
echo 작업이 완료되었습니다. 창을 닫으려면 아무 키나 누르세요.
pause