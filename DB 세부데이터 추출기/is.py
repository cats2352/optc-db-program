import sqlite3
import os
import pyperclip
import csv
import io
from datetime import datetime, timedelta

# --- 상수 정의 (설정) ---
# 테이블 이름과 출력 헤더를 매핑합니다.
TABLE_HEADERS = {
    'MstGasha_': '[스고페스 정보]',
    'MstVoyageQuestMissionLevel_': '[대항해 미션 정보]',
    'MstVoyageQuestMissionReward_': '[대항해 미션보상 정보]',
    'MstVoyagePointReward_': '[대항해 포인트보상 정보]',
    'MstPrizeExchange_': '[교환소 보상 정보]', # 기본 헤더
    'MstKizunaBattleEventRankReward_': '[유대결전 보상정보]',
    'MstKizunaBattleEventAllianceRankReward_': '[유대결전 개인랭킹보상 정보]',
    'MstKizunaBattleEventAllianceRankRateReward_': '[유대결전 동맹랭킹보상 정보]',
    'MstKizunaBattleEventAllianceMissionReward_': '[유대결전 동맹미션보상 정보]',
    'MstQuestGimmickInformation_': '[대항해 패턴정보]',
    'MstBonusEvent_': '[대난투 정보]',
    'MstAssaultRumbleEventRankReward_': '[난전 이벤트 랭킹보상 정보]',
    'MstAssaultRumbleEventAllianceRankRateReward_': '[난전 동맹 랭킹보상 정보]',
    'MstPiratesArenaGpWinBonus_': '[해적 페스티벌 GP 승리 보너스 정보]',
}

# MstPrizeExchange_ 테이블의 storeType_에 따른 특별 헤더를 매핑합니다.
PRIZE_EXCHANGE_HEADERS = {
    'PrizeExchange::TrailEvent': '[해적왕의 궤적 교환소 보상 정보]',
    'PrizeExchange::PiratesArena': '[해적 페스티벌 교환소 보상 정보]',
    'PrizeExchange::MapGame': '[트레저맵 교환소 보상 정보]',
    'PrizeExchange::KizunaProof': '[유대결전 교환소 보상 정보]',
    'PrizeExchange::Jewel': '[레일리 교환소 보상 정보]',
    'PrizeExchange::GachaCoinRed': '[페스코인 교환소 보상 정보]',
}

# --- 헬퍼 함수 ---

def convert_pst_to_kst(pst_time_str):
    """PST 시간 문자열을 KST로 변환합니다. (KST = PST + 17시간)"""
    try:
        dt_obj = datetime.strptime(f"1900/{pst_time_str}", "%Y/%m/%d %H:%M")
        kst_obj = dt_obj + timedelta(hours=17)
        return kst_obj.strftime("%m/%d %H:%M")
    except (ValueError, TypeError):
        return pst_time_str

def find_db_file(directory='.'):
    """현재 디렉토리에서 SQLite DB 파일(.db) 하나를 찾습니다."""
    db_files = [f for f in os.listdir(directory) if f.endswith('.db')]
    if not db_files:
        print("❌ 오류: 폴더에서 .db 파일을 찾을 수 없습니다.")
        return None
    if len(db_files) > 1:
        print(f"알림: 여러 개의 .db 파일이 발견되었습니다. 첫 번째 파일인 '{db_files[0]}'을(를) 사용합니다.")
    return db_files[0]

def get_all_tables(cursor):
    """DB의 모든 테이블 이름을 리스트로 반환합니다."""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [table[0] for table in cursor.fetchall()]

def get_all_columns(cursor, table_name):
    """특정 테이블의 모든 컬럼 이름을 리스트로 반환합니다."""
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    return [row[1] for row in cursor.fetchall()]

# --- 테이블별 데이터 처리 핸들러 함수 ---

def handle_gasha(cursor, table_name):
    """'MstGasha_' 테이블 데이터를 처리합니다."""
    query = 'SELECT "subName_", "displayStartAt_", "displayEndAt_", "gashaType_" FROM "MstGasha_"'
    cursor.execute(query)
    results = [row for row in cursor.fetchall() if row[3] == 'Gacha::Payment']
    if not results: return None, 0

    processed_data = []
    for sub_name, start, end, _ in results:
        time_period = f"{convert_pst_to_kst(start)}~{convert_pst_to_kst(end)}"
        processed_data.append([sub_name, time_period])

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['subName_', '기간(KST)'])
    writer.writerows(processed_data)
    
    return f"{TABLE_HEADERS[table_name]}\n{output.getvalue()}", len(results)

def handle_prize_exchange(cursor, table_name):
    """'MstPrizeExchange_' 테이블 데이터를 처리합니다."""
    query = 'SELECT "title_", "storeType_" FROM "MstPrizeExchange_"'
    cursor.execute(query)
    results = cursor.fetchall()
    if not results: return None, 0

    header_text = TABLE_HEADERS.get(table_name)
    if results:
        # 첫 번째 행의 storeType을 기준으로 헤더를 결정합니다.
        first_store_type = results[0][1]
        header_text = PRIZE_EXCHANGE_HEADERS.get(first_store_type, header_text)

    titles = [row[0] for row in results]
    output_content = "title_\n" + "\n".join(map(str, titles))
    return f"{header_text}\n{output_content}", len(results)

def handle_rank_reward(cursor, table_name):
    """랭킹 보상 관련 테이블들을 공통으로 처리합니다."""
    query = f'SELECT "rankTop_", "description_" FROM "{table_name}"'
    cursor.execute(query)
    results = cursor.fetchall()
    if not results: return None, 0
    
    grouped_data = {}
    for rank, desc in results:
        grouped_data.setdefault(rank, []).append(str(desc))
        
    output_blocks = [f"{rank}위\n" + "\n".join(descs) for rank, descs in sorted(grouped_data.items())]
    return f"{TABLE_HEADERS[table_name]}\n" + "\n\n".join(output_blocks), len(results)

def handle_alliance_rank_rate_reward(cursor, table_name):
    """동맹 랭킹 비율 보상 관련 테이블들을 공통으로 처리합니다."""
    id_column = "kizunaAllianceRankId_" if "Kizuna" in table_name else "assaultRumbleAllianceRankId_"
    query = f'SELECT "{id_column}", "description_" FROM "{table_name}"'
    cursor.execute(query)
    results = cursor.fetchall()
    if not results: return None, 0

    grouped_data = {}
    for rank_id, desc in results:
        grouped_data.setdefault(rank_id, []).append(str(desc))

    output_blocks = [f"{rank_id}위\n" + "\n".join(descs) for rank_id, descs in sorted(grouped_data.items())]
    return f"{TABLE_HEADERS[table_name]}\n" + "\n\n".join(output_blocks), len(results)

def handle_pirates_arena(cursor, table_name):
    """'MstPiratesArenaGpWinBonus_' 테이블 데이터를 처리합니다."""
    query = 'SELECT "winCount_", "description_" FROM "{table_name}"'
    cursor.execute(query)
    results = cursor.fetchall()
    if not results: return None, 0

    grouped_data = {}
    for win_count, desc in results:
        grouped_data.setdefault(win_count, []).append(str(desc))

    output_blocks = [f"{win_count}승\n" + "\n".join(descs) for win_count, descs in sorted(grouped_data.items())]
    return f"{TABLE_HEADERS[table_name]}\n" + "\n\n".join(output_blocks), len(results)

def handle_generic_table(cursor, table_name):
    """특별 핸들러가 없는 모든 일반 테이블을 처리합니다."""
    columns = get_all_columns(cursor, table_name)
    if not columns:
        print(f"❌ 오류: '{table_name}' 테이블에 컬럼이 없습니다.")
        return None, 0

    print(f"\n--- ['{table_name}' 테이블의 컬럼 목록] ---")
    for i, col in enumerate(columns, 1): print(f"{i}. {col}")

    columns_input = input(f"\n[2단계] 추출할 컬럼의 번호를 입력하세요 (여러 개는 쉼표(,), 전체는 * 입력): ")
    
    try:
        if columns_input.strip() == '*':
            columns_to_extract = columns
        else:
            chosen_indices = [int(num.strip()) - 1 for num in columns_input.split(',')]
            if any(not (0 <= i < len(columns)) for i in chosen_indices):
                print(f"❌ 오류: 컬럼 번호가 유효한 범위를 벗어났습니다. (1 ~ {len(columns)})")
                return None, 0
            columns_to_extract = [columns[i] for i in chosen_indices]
    except ValueError:
        print("❌ 오류: 숫자, 쉼표(,), 별표(*)만 사용하여 올바르게 입력해주세요.")
        return None, 0

    columns_for_query = ', '.join([f'"{col}"' for col in columns_to_extract])
    query = f'SELECT {columns_for_query} FROM "{table_name}"'
    print(f"\n실행할 쿼리: {query}")
    
    cursor.execute(query)
    results = cursor.fetchall()
    if not results: return None, 0

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns_to_extract)
    writer.writerows(results)
    
    output_content = output.getvalue()
    header_text = TABLE_HEADERS.get(table_name, "")
    final_output = f"{header_text}\n{output_content}" if header_text else output_content
    return final_output, len(results)

# --- 핸들러 맵 ---
# 테이블 이름과 처리할 핸들러 함수를 연결합니다.
TABLE_HANDLERS = {
    'MstGasha_': handle_gasha,
    'MstPrizeExchange_': handle_prize_exchange,
    'MstKizunaBattleEventRankReward_': handle_rank_reward,
    'MstKizunaBattleEventAllianceRankReward_': handle_rank_reward,
    'MstAssaultRumbleEventRankReward_': handle_rank_reward,
    'MstKizunaBattleEventAllianceRankRateReward_': handle_alliance_rank_rate_reward,
    'MstAssaultRumbleEventAllianceRankRateReward_': handle_alliance_rank_rate_reward,
    'MstPiratesArenaGpWinBonus_': handle_pirates_arena,
}

# --- 메인 실행 로직 ---

def main():
    """메인 로직을 실행합니다."""
    db_file = find_db_file()
    if not db_file:
        return

    try:
        with sqlite3.connect(db_file) as conn:
            print(f"\n✅ 데이터베이스 '{db_file}'에 정상적으로 연결되었습니다.")
            cursor = conn.cursor()
            
            tables = get_all_tables(cursor)
            if not tables:
                print("❌ 오류: 데이터베이스에 테이블이 없습니다.")
                return
                
            print("\n--- [사용 가능한 테이블 목록] ---")
            for i, table in enumerate(tables, 1): print(f"{i}. {table}")
            
            try:
                choice = input("\n[1단계] 위 목록에서 데이터를 추출할 테이블의 번호를 입력하세요: ")
                table_index = int(choice) - 1
                if not (0 <= table_index < len(tables)):
                    print(f"❌ 오류: 1에서 {len(tables)} 사이의 번호를 입력해주세요.")
                    return
                table_name = tables[table_index]
                print(f"✅ 선택된 테이블: '{table_name}'")
            except ValueError:
                print("❌ 오류: 숫자를 입력해야 합니다.")
                return

            # 핸들러 맵에서 적절한 처리 함수를 찾습니다. 없으면 일반 핸들러를 사용합니다.
            handler = TABLE_HANDLERS.get(table_name, handle_generic_table)
            print(f"\n알림: '{handler.__name__}' 핸들러를 사용하여 특별/일반 처리를 시작합니다.")
            
            # 선택된 핸들러를 실행하여 결과를 받습니다.
            final_output, original_row_count = handler(cursor, table_name)

            # 결과 처리 및 출력 (공통 로직)
            if final_output is None:
                print("\n결과 없음: 해당 조건에 맞는 데이터가 없거나 처리 중 오류가 발생했습니다.")
                return

            print("\n--- 📋 추출 완료 ---")
            print(f"총 {original_row_count}개의 원본 행(row) 데이터가 조건에 맞게 처리/추출되었습니다.")
            pyperclip.copy(final_output)
            print("결과가 클립보드에 복사되었습니다!")
            
            print("\n--- 미리보기 ---")
            print(final_output[:500] + ("..." if len(final_output) > 500 else ""))

    except sqlite3.Error as e:
        print(f"❌ 데이터베이스 오류가 발생했습니다: {e}")
    except Exception as e:
        print(f"❌ 알 수 없는 오류가 발생했습니다: {e}")

if __name__ == '__main__':
    main()