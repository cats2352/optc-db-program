import sqlite3
import json
import os
import pyperclip

# --- 1. 상수 정의 (Constants Definition) ---
# 코드의 여러 곳에서 사용되는 테이블 및 컬럼 이름을 상수로 정의하여
# 오타를 줄이고 나중에 변경하기 쉽게 만듭니다.
DB_EVENT_TABLE = "MstEventCharacterBoost_"
DB_CHARACTER_TABLE = "MstCharacter_"
COL_TIMESTAMP = "updateTimestamp_"
COL_CHAR_JSON = "charactersJson_"
COL_SERVER_ID = "serverId_"
COL_LOGBOOK_ID = "logbookId_"


def find_db_file(directory='.'):
    """지정된 디렉토리에서 .db 파일을 찾아 파일 이름을 반환합니다."""
    try:
        # os.scandir가 listdir보다 많은 파일을 다룰 때 더 효율적입니다.
        with os.scandir(directory) as entries:
            db_files = [f.name for f in entries if f.is_file() and f.name.endswith('.db')]
    except FileNotFoundError:
        print(f"❌ 오류: 디렉토리를 찾을 수 없습니다: '{directory}'")
        return None

    if not db_files:
        print("❌ 오류: 스크립트가 있는 폴더에서 .db 파일을 찾을 수 없습니다.")
        return None
    
    if len(db_files) > 1:
        print(f"🔔 알림: 여러 개의 .db 파일이 발견되었습니다. 첫 번째 파일인 '{db_files[0]}'을(를) 사용합니다.")
    
    return db_files[0]

def get_event_character_server_ids(cursor, timestamp):
    """
    주어진 타임스탬프를 사용하여 이벤트 테이블에서 캐릭터 서버 ID 목록을 가져옵니다.
    """
    query = f"SELECT {COL_CHAR_JSON} FROM {DB_EVENT_TABLE} WHERE {COL_TIMESTAMP} = ?"
    cursor.execute(query, (timestamp,))
    results = cursor.fetchall()

    if not results:
        # 결과가 없는 것은 오류가 아니므로, main 함수에서 처리하도록 None을 반환합니다.
        return None
        
    all_server_ids = []
    for row in results:
        # row[0] 값이 비어있는(None) 경우를 대비합니다.
        if row[0] is None:
            continue
        json_data = json.loads(row[0])
        # .get()을 사용하면 'character_ids' 키가 없을 때도 오류 없이 안전하게 처리됩니다.
        server_ids_in_row = json_data.get('character_ids', [])
        all_server_ids.extend(server_ids_in_row)

    return all_server_ids

def map_server_ids_to_logbook_ids(cursor, server_ids):
    """
    서버 ID 목록을 로그북 ID 목록으로 변환합니다.
    """
    if not server_ids:
        return []

    # SQL 인젝션 공격을 방지하면서 동적으로 파라미터를 생성합니다.
    placeholders = ','.join(['?'] * len(server_ids))
    query = f"SELECT {COL_SERVER_ID}, {COL_LOGBOOK_ID} FROM {DB_CHARACTER_TABLE} WHERE {COL_SERVER_ID} IN ({placeholders})"
    
    cursor.execute(query, server_ids)
    # Dictionary comprehension을 사용하여 ID 매핑을 더 간결하게 생성합니다.
    id_map = {server_id: logbook_id for server_id, logbook_id in cursor.fetchall()}
    
    # server_ids 목록을 순회하며 매핑되는 logbook_id를 찾습니다.
    logbook_ids = [id_map[sid] for sid in server_ids if sid in id_map]
    
    return logbook_ids

def format_ids_for_clipboard(logbook_ids):
    """
    로그북 ID 목록을 정렬하고, 10개씩 묶어 클립보드에 복사할 형식의 문자열로 만듭니다.
    """
    if not logbook_ids:
        return ""

    logbook_ids.sort()

    output_lines = []
    # 리스트를 10개 단위로 나누는 간결한 방법입니다.
    chunks = [logbook_ids[i:i + 10] for i in range(0, len(logbook_ids), 10)]
    
    for chunk in chunks:
        # map(str, chunk)를 사용하여 각 숫자를 문자열로 변환하고 join으로 합칩니다.
        number_part = ", ".join(map(str, chunk)) + ","
        indented_line = f"\t\t\t\t\t{number_part}"
        output_lines.append(indented_line)
    
    return "\n".join(output_lines)

def main():
    """
    스크립트의 메인 실행 함수.
    사용자 입력을 받고, 데이터베이스 작업을 수행하며, 결과를 클립보드에 복사합니다.
    """
    try:
        db_filename = find_db_file()
        if not db_filename:
            return

        timestamp_input = input("추출할 updateTimestamp_ 값을 입력하세요: ")
        if not timestamp_input.strip():
            print("🔔 알림: 작업이 취소되었습니다.")
            return

        # 'with' 구문을 사용하여 데이터베이스 연결을 안전하게 관리합니다.
        with sqlite3.connect(db_filename) as conn:
            print(f"\n✅ 데이터베이스 '{db_filename}'에 연결되었습니다.")
            cursor = conn.cursor()

            server_ids = get_event_character_server_ids(cursor, timestamp_input)
            
            if server_ids is None:
                print(f"🤷 결과 없음: 입력한 타임스탬프 '{timestamp_input}'에 해당하는 이벤트를 찾을 수 없습니다.")
                return
            if not server_ids:
                print("ℹ️ 정보: 해당 이벤트에 지정된 캐릭터가 없습니다.")
                return

            logbook_ids = map_server_ids_to_logbook_ids(cursor, server_ids)
            
            if not logbook_ids:
                print("❌ 오류: 캐릭터의 서버 ID를 로그북 ID로 변환하는 데 실패했습니다.")
                return

        # 데이터베이스 작업이 모두 끝난 후 결과를 처리합니다.
        final_output = format_ids_for_clipboard(logbook_ids)
        
        if final_output:
            pyperclip.copy(final_output)
            print("\n--- 📋 복사 완료 ---")
            print(f"{len(logbook_ids)}개의 ID가 들여쓰기 적용되어 복사되었습니다.")
            print("\n(일부 미리보기)")
            # 미리보기가 200자를 넘어가면 '...'을 붙여줍니다.
            preview = (final_output[:200] + '...') if len(final_output) > 200 else final_output
            print(preview)
        else:
            print("ℹ️ 정보: 클립보드에 복사할 ID가 없습니다.")

    # 각 예외 유형에 따라 구체적인 오류 메시지를 출력합니다.
    except sqlite3.Error as e:
        print(f"❌ 데이터베이스 오류: 데이터베이스 처리 중 오류가 발생했습니다:\n{e}")
    except json.JSONDecodeError:
        print("❌ JSON 오류: 데이터베이스의 charactersJson_ 형식이 잘못되었습니다.")
    except pyperclip.PyperclipException as e:
        print(f"❌ 클립보드 오류: 클립보드에 접근할 수 없습니다. 'pyperclip' 라이브러리가 올바르게 설치되었는지 확인하세요.\n{e}")
    except Exception as e:
        print(f"❌ 알 수 없는 오류: 예상치 못한 오류가 발생했습니다:\n{e}")

if __name__ == "__main__":
    main()