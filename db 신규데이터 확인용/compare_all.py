import sqlite3
import os

def find_db_files(directory='.'):
    """현재 디렉토리에서 SQLite DB 파일(.db, .sqlite) 두 개를 찾습니다."""
    print(f"--- 1. 현재 폴더({os.path.abspath(directory)})에서 파일 찾는 중 ---")
    all_files = os.listdir(directory)
    print(f"발견된 모든 파일: {all_files}")

    # 결과 파일 이름은 제외하고 찾습니다.
    db_files = [f for f in all_files if f.endswith(('.db', '.sqlite')) and f != '추가목록.db']

    if len(db_files) < 2:
        print("\n!!! 오류: 비교할 DB 파일이 2개 미만입니다. 폴더에 .db 또는 .sqlite 파일 2개가 있는지 확인해 주세요.")
        return None, None

    print(f"성공: DB 파일 2개를 찾았습니다.")
    print(f"'{db_files[0]}' (기존 DB)와 '{db_files[1]}' (신규 DB)를 비교합니다.")
    return db_files[0], db_files[1]

def get_table_names(conn):
    """데이터베이스의 모든 테이블 이름을 가져옵니다."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]
    return tables

# <-- 추가된 함수: 테이블의 컬럼 이름을 가져옵니다.
def get_column_names(conn, table_name):
    """특정 테이블의 모든 컬럼 이름을 리스트로 반환합니다."""
    cursor = conn.cursor()
    # PRAGMA table_info는 테이블의 각 컬럼에 대한 정보를 반환합니다.
    # 결과의 두 번째 값(인덱스 1)이 컬럼 이름입니다.
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    return [row[1] for row in cursor.fetchall()]

def compare_and_extract(db1_path, db2_path, result_path):
    """두 데이터베이스를 비교하고 추가된 데이터를 새 DB에 저장합니다."""
    conn1, conn2, result_conn = None, None, None # 변수 초기화
    try:
        # 데이터베이스 연결
        conn1 = sqlite3.connect(db1_path)
        conn2 = sqlite3.connect(db2_path)
        result_conn = sqlite3.connect(result_path)

        print("\n--- 2. 각 데이터베이스에서 테이블 목록 가져오는 중 ---")
        tables1 = set(get_table_names(conn1))
        tables2 = set(get_table_names(conn2))
        print(f"'{db1_path}'의 테이블: {list(tables1)}")
        print(f"'{db2_path}'의 테이블: {list(tables2)}")

        common_tables = sorted(list(tables1.intersection(tables2)))

        if not common_tables:
            print("\n!!! 경고: 두 데이터베이스에 이름이 같은 테이블이 하나도 없습니다. 작업을 종료합니다.")
            return

        print(f"\n--- 3. 공통 테이블 비교 시작 ---")
        print(f"공통 테이블: {common_tables}")

        for table_name in common_tables:
            print(f"\n▶ '{table_name}' 테이블 비교 중...")

            # <-- 수정된 부분: 기준 컬럼을 동적으로 결정합니다.
            key_column = None
            table_columns = get_column_names(conn1, table_name) # conn1 기준으로 컬럼 확인

            if 'serverId_' in table_columns:
                key_column = 'serverId_'
            elif 'updateTimestamp_' in table_columns:
                key_column = 'updateTimestamp_'

            if not key_column:
                print(f"   !!! 경고: '{table_name}' 테이블에 'serverId_' 또는 'updateTimestamp_' 컬럼이 없어 건너뜁니다.")
                continue # 다음 테이블로 넘어감

            print(f"   -> 기준 컬럼 '{key_column}'(으)로 비교를 시작합니다.")
            # <-- 여기까지 수정된 부분

            try:
                # 각 테이블의 ID 목록을 집합(set)으로 가져오기
                cursor1 = conn1.cursor()
                cursor1.execute(f'SELECT "{key_column}" FROM "{table_name}"')
                ids1 = {row[0] for row in cursor1.fetchall()}
                print(f"   -> 조회된 ID {len(ids1)}개")

                cursor2 = conn2.cursor()
                cursor2.execute(f'SELECT "{key_column}" FROM "{table_name}"')
                ids2 = {row[0] for row in cursor2.fetchall()}
                print(f"   -> 조회된 ID {len(ids2)}개")

                added_ids = sorted(list(ids2 - ids1))

                if not added_ids:
                    print(f"   -> 추가된 ID 없음")
                    continue

                print(f"   -> {len(added_ids)}개의 추가된 ID 발견: {added_ids}")
                
                cursor2.execute(f'SELECT * FROM "{table_name}" WHERE "{key_column}" IN ({",".join("?" for _ in added_ids)})', added_ids)
                new_data = cursor2.fetchall()
                
                result_cursor = result_conn.cursor()
                cursor2.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                create_table_sql = cursor2.fetchone()[0]
                result_cursor.execute(f"DROP TABLE IF EXISTS \"{table_name}\"")
                result_cursor.execute(create_table_sql)

                if new_data:
                    placeholders = ', '.join(['?'] * len(new_data[0]))
                    result_cursor.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', new_data)
                
                result_conn.commit()
                print(f"   -> '{table_name}' 테이블에 추가된 데이터 저장 완료")

            except sqlite3.Error as e:
                print(f"   !!! 데이터베이스 오류 발생: 이 테이블은 건너뜁니다.")
                print(f"   -> 전체 오류 메시지: {e}")

    except Exception as e:
        print(f"스크립트 실행 중 예기치 않은 오류 발생: {e}")
    finally:
        if conn1: conn1.close()
        if conn2: conn2.close()
        if result_conn: result_conn.close()
        print("\n--- 4. 모든 작업 완료 ---")

if __name__ == '__main__':
    db1, db2 = find_db_files()
    
    if db1 and db2:
        output_db = '추가목록.db'
        compare_and_extract(db1, db2, output_db)