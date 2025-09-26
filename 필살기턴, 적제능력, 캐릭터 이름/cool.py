import sqlite3
import os
import pyperclip

def find_db_file(directory='.'):
    """현재 디렉토리에서 SQLite DB 파일(.db) 하나를 찾습니다."""
    db_files = [f for f in os.listdir(directory) if f.endswith('.db')]
    
    if not db_files:
        print("오류: 폴더에서 .db 파일을 찾을 수 없습니다.")
        return None
    
    if len(db_files) > 1:
        print(f"경고: 여러 개의 .db 파일({db_files})이 발견되었습니다. 첫 번째 파일 '{db_files[0]}'을 사용합니다.")
    
    return db_files[0]

def main():
    """메인 로직을 실행합니다."""
    db_file = find_db_file()
    if not db_file:
        return

    print(f"데이터베이스 '{db_file}' 파일을 사용합니다.")
    
    print("\n--- logbookId_ 범위 입력 ---")
    start_id_str = input("시작 ID를 입력하세요: ")
    end_id_str = input("종료 ID를 입력하세요: ")

    if not (start_id_str.strip().isdigit() and end_id_str.strip().isdigit()):
        print("\n❌ 오류: ID는 숫자로만 입력해야 합니다. 프로그램을 종료합니다.")
        return

    id1 = int(start_id_str)
    id2 = int(end_id_str)

    start_logbook_id = min(id1, id2)
    end_logbook_id = max(id1, id2)
    
    print(f"\n지정된 ID 범위 {start_logbook_id} ~ {end_logbook_id}의 데이터를 조회합니다...")

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        all_output_lines = []
        
        for current_logbook_id in range(start_logbook_id, end_logbook_id + 1):
            
            cursor.execute('SELECT serverId_ FROM MstCharacter_ WHERE logbookId_ = ?', (current_logbook_id,))
            server_id_result = cursor.fetchone()

            if not server_id_result:
                print(f"-> 정보: logbookId '{current_logbook_id}'에 해당하는 캐릭터가 없어 건너뜁니다.")
                continue

            server_id = server_id_result[0]
            
            cursor.execute('SELECT turn_, maxLevel_ FROM MstAbility_ WHERE serverId_ = ? ORDER BY turn_', (server_id,))
            ability_results = cursor.fetchall()

            if not ability_results:
                print(f"-> 정보: logbookId '{current_logbook_id}'(serverId: {server_id})의 Ability 정보가 없어 건너뜁니다.")
                continue

            for turn, max_level in ability_results:
                if max_level is None:
                    max_level = 0
                
                calculated_value = turn - max_level + 1
                # <-- 수정된 부분: 맨 앞에 공백 4칸을 추가하여 들여쓰기를 합니다.
                formatted_line = f"    [{turn}, {calculated_value}],"
                all_output_lines.append(formatted_line)
        
        if not all_output_lines:
            print("\n해당 범위에서 변환할 데이터를 찾지 못했습니다.")
            return

        final_output = "\n".join(all_output_lines)

        print("\n--- 변환된 데이터 ---")
        print(final_output)

        pyperclip.copy(final_output)
        print("\n결과가 클립보드에 복사되었습니다! 🎉")

    except sqlite3.Error as e:
        print(f"데이터베이스 오류가 발생했습니다: {e}")
    except pyperclip.PyperclipException:
        print("\n(참고: pyperclip 라이브러리가 없거나 오류가 발생하여 클립보드에 복사하지 못했습니다.)")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()