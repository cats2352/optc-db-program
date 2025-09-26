# --- 필요한 라이브러리 불러오기 ---
import sqlite3      # SQLite DB에 연결하기 위한 라이브러리
import json         # 파이썬 객체를 JSON 문자열로 변환하기 위한 라이브러리
import pyperclip    # 클립보드에 텍스트를 복사하기 위한 라이브러리
import os           # 파일 시스템(폴더 내 파일 목록 등)에 접근하기 위한 라이브러리

# --- 데이터 매핑을 위한 딕셔너리 정의 ---
# 데이터베이스의 숫자 코드를 사람이 읽을 수 있는 문자열로 변환(매핑)하는 데 사용됩니다.

# characterType_, subCharacterType_ 매핑
CLASS_MAP = {
    1: "Fighter",
    2: "Slasher",
    3: "Striker",
    4: "Shooter",
    5: "Free Spirit",
    6: "Driven",
    7: "Cerebral",
    8: "Powerhouse"
}

# attributeId_ 매핑
TYPE_MAP = {
    1: "STR",
    2: "DEX",
    3: "QCK",
    4: "PSY",
    5: "INT"
}

def find_database_file():
    """
    현재 스크립트가 실행되는 폴더에서 .db, .sqlite, .sqlite3 확장자를 가진 데이터베이스 파일을 찾습니다.
    """
    # 현재 폴더의 모든 파일을 확인하며, DB 확장자로 끝나는 파일만 리스트에 담습니다.
    db_files = [f for f in os.listdir('.') if f.endswith(('.db', '.sqlite', '.sqlite3'))]
    
    # DB 파일이 하나도 없으면 None을 반환합니다.
    if not db_files:
        return None
        
    # DB 파일이 여러 개 발견되면, 첫 번째 파일을 사용하고 사용자에게 경고 메시지를 보여줍니다.
    if len(db_files) > 1:
        print(f"⚠️  경고: 폴더에 DB 파일이 여러 개 있습니다. '{db_files[0]}' 파일을 사용합니다.")
        
    # 찾은 DB 파일 목록에서 첫 번째 파일의 이름을 반환합니다.
    return db_files[0]

def get_dual_type(cursor, sub_name):
    """
    attributeId_가 9일 경우, subName_을 기준으로 다른 캐릭터 2명의 속성을 찾아 리스트로 반환합니다.
    """
    # subName_이 일치하는 캐릭터의 attributeId_를 2개까지 찾는 SQL 쿼리
    query = "SELECT attributeId_ FROM MstCharacter_ WHERE subName_ = ? LIMIT 2"
    cursor.execute(query, (sub_name,))
    rows = cursor.fetchall()
    # 2개의 속성을 찾아 각각 TYPE_MAP을 이용해 매핑하여 리스트로 만듭니다.
    return [TYPE_MAP.get(row[0], "Unknown") for row in rows]

def process_character_data(cursor, character_ids):
    """
    주어진 캐릭터 ID 목록에 대해 데이터를 조회하고 지정된 양식으로 가공합니다.
    """
    # 처리할 ID가 없으면 빈 리스트를 반환하고 함수 종료
    if not character_ids:
        return []

    # 여러 ID를 한 번에 효율적으로 조회하기 위한 SQL 쿼리 준비 (예: 'IN (?, ?, ?)')
    placeholders = ', '.join(['?'] * len(character_ids))
    query = f"SELECT * FROM MstCharacter_ WHERE serverId_ IN ({placeholders})"
    
    # 데이터베이스에 쿼리 실행
    cursor.execute(query, character_ids)
    rows = cursor.fetchall()

    # 나중에 ID로 데이터를 쉽게 찾기 위해 딕셔너리 형태로 변환 (예: {1: row_data, 2: row_data})
    results_by_id = {row['serverId_']: row for row in rows}
    
    # 최종 결과물을 담을 빈 리스트
    final_data_list = []

    # 사용자가 입력한 ID 순서대로 처리 시작
    for char_id in character_ids:
        # 만약 DB에서 해당 ID의 캐릭터를 찾지 못했다면 경고 메시지를 출력하고 다음 ID로 넘어감
        if char_id not in results_by_id:
            print(f"⚠️ 경고: DB에서 ID {char_id}를 찾을 수 없습니다.")
            continue

        # 현재 처리 중인 캐릭터의 데이터
        char_data = results_by_id[char_id]
        
        # [양식 처리 1] Name, subName 처리: subName이 있으면 "Name - subName" 형태로 조합
        name = char_data['name_']
        if char_data['subName_']:
            name = f"{name} - {char_data['subName_']}"

        # [양식 처리 2] Type 처리: attributeId가 9이면 특별 함수 호출, 아니면 일반 매핑
        type_val = char_data['attributeId_']
        char_type = get_dual_type(cursor, char_data['subName_']) if type_val == 9 else TYPE_MAP.get(type_val, "Unknown")

        # [양식 처리 3] Class 처리: subCharacterType이 유효할 때만 리스트로, 아니면 단일 값으로 처리
        class1_id = char_data['characterType_']
        class2_id = char_data['subCharacterType_']
        class1 = CLASS_MAP.get(class1_id, "Unknown")
        
        if class2_id and class2_id in CLASS_MAP:
            class2 = CLASS_MAP.get(class2_id)
            char_class = [class1, class2]
        else:
            char_class = class1
        
        # [양식 처리 4] Stars 처리: isRarityPlus가 0이 아니면 '+'를 붙이고, 0이면 숫자 그대로 사용
        if char_data['isRarityPlus_'] != 0:
            stars = f"{char_data['rarity_']}+"  # +가 붙으면 문자열
        else:
            stars = char_data['rarity_']  # +가 없으면 숫자
            
        # [양식 처리 5] 최종 양식에 맞춰 16개 항목을 가진 리스트 생성
        processed_list = [
            name, char_type, char_class, stars, char_data['cost_'],
            char_data['comboNum_'], char_data['maxOptionSkill_'], char_data['maxLevel_'],
            char_data['limitExp_'], char_data['minHealth_'], char_data['minAttackDamage_'],
            char_data['minRestoration_'], char_data['maxHealth_'], char_data['maxAttackDamage_'],
            char_data['maxRestoration_'], 1  # Growth Rate는 1로 고정
        ]
        # 완성된 캐릭터 데이터를 최종 결과 리스트에 추가
        final_data_list.append(processed_list)

    # 모든 처리가 끝난 최종 데이터 리스트를 반환
    return final_data_list

def main():
    """
    스크립트의 메인 실행 함수. DB 연결, 사용자 입력, 데이터 처리, 결과 출력 및 복사를 담당합니다.
    """
    # 하드코딩된 경로 대신, 새로 만든 함수를 호출하여 DB 파일 이름을 자동으로 가져옵니다.
    DATABASE_PATH = find_database_file()

    # DB 파일을 찾지 못했을 경우, 메시지를 보여주고 스크립트를 종료합니다.
    if not DATABASE_PATH:
        print("❌ 폴더에서 데이터베이스 파일(.db, .sqlite, .sqlite3)을 찾을 수 없습니다.")
        return

    # try...finally 구문: 프로그램 실행 중 오류가 발생하더라도 항상 DB 연결을 닫도록 보장합니다.
    try:
        # 데이터베이스에 연결합니다.
        con = sqlite3.connect(DATABASE_PATH)
        # DB의 컬럼 이름으로 데이터에 접근할 수 있도록 설정합니다. (예: row['serverId_'])
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        
        # 사용자에게 캐릭터 번호 입력을 요청합니다.
        print("데이터를 추출할 캐릭터의 번호를 입력하세요 (여러 개는 쉼표(,)로 구분).")
        input_ids_str = input("캐릭터 번호: ")
        
        # 입력된 문자열을 쉼표로 나누고, 각 항목을 정수(숫자)로 변환하여 리스트로 만듭니다.
        character_ids = [int(id_str.strip()) for id_str in input_ids_str.split(',')]
        
        # 핵심 데이터 처리 함수를 호출하여 결과를 받습니다.
        extracted_data = process_character_data(cur, character_ids)
        
        # 터미널에 결과 출력
        print("\n--- 추출된 데이터 ---")

        # 최종 출력 및 복사를 위한 문자열들을 담을 리스트
        final_strings = []

        # 추출된 각 캐릭터의 데이터를 하나씩 순회합니다.
        for char_data in extracted_data:
            # 1. 캐릭터 한 명의 데이터를 한 줄짜리 JSON 문자열로 변환합니다.
            json_string = json.dumps(char_data, ensure_ascii=False)
            # 2. Class 목록의 괄호 안쪽에 공백을 추가합니다: ["A","B"] -> [ "A","B" ]
            json_string = json_string.replace('["', '[ "').replace('"]', '" ]')
            # 3. 맨 처음과 맨 끝 괄호를 제외한 '내용물'만 추출합니다.
            content = json_string[1:-1]
            # 4. f-string을 사용해 원하는 모든 서식을 한 번에 적용하여 최종 문자열을 조립합니다.
            final_string_for_char = f"\t[{content} ],"
            final_strings.append(final_string_for_char)

        # 각 줄의 문자열들을 줄바꿈(\n)으로 합쳐서 최종 결과물 생성
        output_string = "\n".join(final_strings)

        # 터미널에 최종 결과물 출력
        print(output_string)
        
        # 추출된 데이터가 있을 경우에만 클립보드에 복사
        if extracted_data:
            pyperclip.copy(output_string)
            print("\n✅ 데이터가 클립보드에 복사되었습니다.")
        
    # 예외 처리: 다양한 종류의 오류를 처리하여 프로그램이 갑자기 종료되지 않도록 합니다.
    except sqlite3.Error as e:
        print(f"❌ 데이터베이스 오류가 발생했습니다: {e}")
    except ValueError:
        print("❌ 잘못된 숫자 형식입니다. 번호와 쉼표(,)만 사용하여 입력해주세요.")
    # 'con' 변수가 성공적으로 생성되었을 때만 con.close()를 호출하도록 수정
    finally:
        if 'con' in locals() and con:
            con.close() # DB 연결을 닫습니다.

# 이 스크립트 파일이 직접 실행되었을 때만 main() 함수를 호출합니다.
if __name__ == "__main__":
    main()