import sqlite3
import json
import os
import shutil
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

def convert_and_extract_images_from_subfolder():
    """
    MstEventCharacterBoost_ 테이블의 serverId를 입력받아,
    해당 row의 charactersJson_ 안의 모든 ID를 logbookId로 변환하고,
    변환된 ID와 일치하는 png 이미지를 'jap' 하위 폴더에서 찾아 별도 폴더에 복사한다.
    """
    try:
        # --- 1. DB 파일 자동 찾기 ---
        db_filename = find_db_file()
        if not db_filename:
            return

        # --- 2. 터미널에서 사용자 입력받기 ---
        server_id_input = input("이벤트를 찾을 기준 serverId 한 개를 입력하세요 (MstEventCharacterBoost_ 테이블 기준): ")

        if not server_id_input.strip().isdigit():
            print("\n입력 오류: 올바른 Server ID(숫자)를 입력해주세요.")
            return

        # --- 3. 데이터베이스 연결 ---
        conn = sqlite3.connect(db_filename)
        print(f"\n✅ 성공: 데이터베이스 '{db_filename}'에 정상적으로 연결되었습니다.")
        cursor = conn.cursor()
        
        cursor.execute("SELECT charactersJson_ FROM MstEventCharacterBoost_ WHERE serverId_ = ?", (server_id_input,))
        result = cursor.fetchone()

        if not result or result[0] is None:
            print(f"\n결과 없음: MstEventCharacterBoost_ 테이블에서 Server ID '{server_id_input}'를 찾을 수 없거나 해당 이벤트에 JSON 데이터가 없습니다.")
            conn.close()
            return
            
        try:
            json_data = json.loads(result[0])
            ids_to_convert = json_data.get('character_ids', [])
        except json.JSONDecodeError:
            print("\nJSON 오류: 데이터베이스의 charactersJson_ 형식이 잘못되었습니다.")
            conn.close()
            return

        if not ids_to_convert:
            print("\n정보: JSON 데이터 안에 변환할 캐릭터 ID가 없습니다.")
            conn.close()
            return
            
        placeholders = ','.join(['?'] * len(ids_to_convert))
        query = f"SELECT serverId_, logbookId_ FROM MstCharacter_ WHERE serverId_ IN ({placeholders})"
        cursor.execute(query, ids_to_convert)
        id_map = {server_id: logbook_id for server_id, logbook_id in cursor.fetchall()}
        conn.close()
        
        logbook_ids = sorted([id_map.get(sid) for sid in ids_to_convert if id_map.get(sid) is not None])

        if not logbook_ids:
            print("\n변환 오류: JSON 안의 ID들을 logbook ID로 변환하는 데 실패했습니다.")
            return

        # --- 텍스트 복사 로직 ---
        output_lines = []
        chunks = [logbook_ids[i:i + 10] for i in range(0, len(logbook_ids), 10)]
        for chunk in chunks:
            number_part = ", ".join(map(str, chunk)) + ","
            indented_line = f"\t\t\t\t{number_part}"
            output_lines.append(indented_line)
        final_output = "\n".join(output_lines)
        pyperclip.copy(final_output)
        
        # --- 이미지 추출 로직 ---
        script_folder = os.path.dirname(os.path.abspath(__file__))
        image_source_folder = os.path.join(script_folder, "jap")
        destination_folder = os.path.join(script_folder, "추출된_이미지")
        
        os.makedirs(destination_folder, exist_ok=True)
        
        copied_files_count = 0
        for logbook_id in logbook_ids:
            image_filename = f"{logbook_id}.png"
            source_path = os.path.join(image_source_folder, image_filename)
            
            if os.path.exists(source_path):
                destination_path = os.path.join(destination_folder, image_filename)
                shutil.copy2(source_path, destination_path)
                copied_files_count += 1
        
        # --- 최종 성공 메시지 ---
        print("\n--- ✅ 작업 완료 ---")
        print(f"총 {len(logbook_ids)}개의 ID가 텍스트로 변환되어 클립보드에 복사되었습니다.")
        print(f"또한, 'jap' 폴더에서 일치하는 이미지 {copied_files_count}개를 '추출된_이미지' 폴더에 복사했습니다.")

    except sqlite3.Error as e:
        print(f"\n데이터베이스 오류: 데이터베이스 처리 중 오류가 발생했습니다:\n{e}")
    except Exception as e:
        print(f"\n알 수 없는 오류: 알 수 없는 오류가 발생했습니다:\n{e}")

if __name__ == "__main__":
    convert_and_extract_images_from_subfolder()