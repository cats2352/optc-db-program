import sqlite3
import pyperclip
import os

# piratesStyle_ 값에 따라 변환될 텍스트 딕셔너리
STYLE_MAP = {
    0: "BAL",
    1: "ATK",
    2: "DEF",
    3: "RCV",
    4: "SPT",
    5: "DBF"
}

def find_db_file(directory='.'):
    """
    현재 디렉토리에서 .db 파일을 찾아 파일 이름을 반환합니다.
    """
    db_files = [f for f in os.listdir(directory) if f.endswith('.db')]
    
    if not db_files:
        print("오류: 스크립트가 있는 폴더에서 .db 파일을 찾을 수 없습니다.")
        return None
    
    if len(db_files) > 1:
        print(f"알림: 여러 개의 .db 파일이 발견되었습니다. 첫 번째 파일인 '{db_files[0]}'을(를) 사용합니다.")
    
    return db_files[0]

def get_character_data_and_copy():
    """
    터미널에서 ID 범위를 입력받아 DB 데이터를 조회하고,
    지정된 형식으로 변환하여 클립보드에 복사하는 함수입니다.
    """
    try:
        db_filename = find_db_file()
        if not db_filename:
            return

        # --- 터미널(실행창)에서 사용자 입력받기 ---
        print("\n--- ID 범위 입력 ---")
        start_id_str = input("시작 logbookId_를 입력하세요: ")
        end_id_str = input("종료 logbookId_를 입력하세요: ")

        # 입력값이 숫자인지 확인
        if not (start_id_str.strip().isdigit() and end_id_str.strip().isdigit()):
            print("\n❌ 오류: ID는 숫자로만 입력해야 합니다. 프로그램을 종료합니다.")
            return

        # 입력받은 텍스트를 정수(숫자)로 변환
        id1 = int(start_id_str)
        id2 = int(end_id_str)
        
        # --- 데이터베이스 연결 ---
        conn = sqlite3.connect(db_filename)
        print(f"\n✅ 성공: 데이터베이스 '{db_filename}'에 정상적으로 연결되었습니다.")

        cursor = conn.cursor()

        # 사용자가 순서를 바꿔 입력해도 괜찮도록 min, max로 자동 정렬
        start_id = min(id1, id2)
        end_id = max(id1, id2)
        print(f"지정된 ID 범위 {start_id} ~ {end_id}의 데이터를 조회합니다...")

        query = """
            SELECT piratesStyle_, piratesDefense_, piratesSpeed_ 
            FROM MstCharacter_ 
            WHERE logbookId_ BETWEEN ? AND ?
            ORDER BY logbookId_
        """
        cursor.execute(query, (start_id, end_id))
        results = cursor.fetchall()
        
        conn.close()

        if results:
            output_lines = []
            
            for row in results:
                pirates_style_val, pirates_defense, pirates_speed = row
                style_str = STYLE_MAP.get(pirates_style_val, "UNKNOWN")
                
                # <-- 수정된 부분: 맨 앞에 공백 4칸을 추가하여 들여쓰기를 합니다.
                line = f'    ["{style_str}", {pirates_defense}, {pirates_speed}],'
                output_lines.append(line)
            
            final_output = "\n".join(output_lines)
            
            pyperclip.copy(final_output)
            
            # 터미널에 결과 출력
            print("\n--- 📋 복사 완료 ---")
            print(f"{len(results)}개의 데이터가 클립보드에 복사되었습니다:")
            print(final_output)
        else:
            print(f"\n결과 없음: ID 범위 {start_id} ~ {end_id}에 해당하는 캐릭터를 찾을 수 없습니다.")

    except sqlite3.Error as e:
        print(f"데이터베이스 오류: 데이터베이스 처리 중 오류가 발생했습니다:\n{e}")
    except Exception as e:
        print(f"알 수 없는 오류: 알 수 없는 오류가 발생했습니다:\n{e}")

if __name__ == "__main__":
    get_character_data_and_copy()