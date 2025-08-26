import sqlite3
import json
import os
import pyperclip

def find_db_file(directory='.'):
    """
    현재 디렉토리에서 .db 파일을 찾아 파일 이름을 반환합니다.
    """
    db_files = [f for f in os.listdir(directory) if f.endswith('.db')]
    
    if not db_files:
        print("❌ 오류: 스크립트가 있는 폴더에서 .db 파일을 찾을 수 없습니다.")
        return None
    
    if len(db_files) > 1:
        print(f"알림: 여러 개의 .db 파일이 발견되었습니다. 첫 번째 파일인 '{db_files[0]}'을(를) 사용합니다.")
    
    return db_files[0]

def copy_boost_characters():
    """
    터미널에서 updateTimestamp_를 입력받아 캐릭터 ID를 변환하고 클립보드에 복사합니다.
    """
    try:
        # --- 1. DB 파일 자동 찾기 ---
        db_filename = find_db_file()
        if not db_filename:
            return

        # --- 2. 터미널에서 타임스탬프 입력받기 ---
        timestamp_input = input("추출할 updateTimestamp_ 값을 입력하세요: ")

        if not timestamp_input.strip():
            print("알림: 작업이 취소되었습니다.")
            return

        # --- 3. 데이터베이스 연결 ---
        conn = sqlite3.connect(db_filename)
        print(f"\n✅ 데이터베이스 '{db_filename}'에 연결되었습니다.")
        cursor = conn.cursor()

        cursor.execute("SELECT charactersJson_ FROM MstEventCharacterBoost_ WHERE updateTimestamp_ = ?", (timestamp_input,))
        results = cursor.fetchall()

        if not results:
            print(f"결과 없음: 입력한 타임스탬프 '{timestamp_input}'에 해당하는 이벤트를 찾을 수 없습니다.")
            conn.close()
            return
            
        all_server_ids = []
        for row in results:
            json_data = json.loads(row[0])
            server_ids_in_row = json_data.get('character_ids', [])
            all_server_ids.extend(server_ids_in_row)

        if not all_server_ids:
            print("정보: 해당 이벤트에 지정된 캐릭터가 없습니다.")
            conn.close()
            return
            
        placeholders = ','.join(['?'] * len(all_server_ids))
        query = f"SELECT serverId_, logbookId_ FROM MstCharacter_ WHERE serverId_ IN ({placeholders})"
        
        cursor.execute(query, all_server_ids)
        id_map = {server_id: logbook_id for server_id, logbook_id in cursor.fetchall()}
        
        conn.close()
        
        logbook_ids = [id_map.get(sid) for sid in all_server_ids if id_map.get(sid) is not None]
        
        logbook_ids.sort()

        if not logbook_ids:
            print("오류: 캐릭터 ID를 logbook ID로 변환하는 데 실패했습니다.")
            return

        output_lines = []
        chunks = [logbook_ids[i:i + 10] for i in range(0, len(logbook_ids), 10)]
        
        for chunk in chunks:
            number_part = ", ".join(map(str, chunk)) + ","
            indented_line = f"\t\t\t\t\t{number_part}"
            output_lines.append(indented_line)
        
        final_output = "\n".join(output_lines)

        pyperclip.copy(final_output)
        
        print("\n--- 📋 복사 완료 ---")
        print(f"{len(logbook_ids)}개의 ID가 들여쓰기 적용되어 복사되었습니다.")
        print("\n(일부 미리보기)")
        print(final_output[:200])

    except sqlite3.Error as e:
        print(f"데이터베이스 오류: 데이터베이스 처리 중 오류가 발생했습니다:\n{e}")
    except json.JSONDecodeError:
        print("JSON 오류: 데이터베이스의 charactersJson_ 형식이 잘못되었습니다.")
    except Exception as e:
        print(f"알 수 없는 오류: 알 수 없는 오류가 발생했습니다:\n{e}")

if __name__ == "__main__":
    copy_boost_characters()