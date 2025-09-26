import sqlite3
import os

def find_db_file(output_filename, directory='.'):
    """
    현재 디렉토리에서 SQLite DB 파일을 찾아 파일 이름을 반환합니다.
    결과 파일 이름은 검색에서 제외합니다.
    """
    db_files = [f for f in os.listdir(directory) if f.endswith(('.db', '.sqlite')) and f != output_filename]
    
    if not db_files:
        print(f"❌ 오류: 폴더에서 원본 DB 파일을 찾을 수 없습니다. (결과 파일 '{output_filename}'은 제외하고 검색합니다)")
        return None
    
    if len(db_files) > 1:
        print(f"알림: 여러 개의 DB 파일이 발견되었습니다. 첫 번째 파일인 '{db_files[0]}'을(를) 사용합니다.")
    
    return db_files[0]

def get_all_tables(conn):
    """DB의 모든 테이블 이름을 리스트로 반환합니다."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [table[0] for table in cursor.fetchall()]

def extract_tables():
    """사용자로부터 테이블을 선택받아 새로운 DB 파일로 추출합니다."""
    output_db_name = 'extracted_tables.db'
    
    # 1. 원본 DB 파일 찾기
    source_db_name = find_db_file(output_db_name)
    if not source_db_name:
        return

    source_conn = None
    dest_conn = None
    try:
        # 2. 원본 DB에 연결하고 테이블 목록 보여주기
        source_conn = sqlite3.connect(source_db_name)
        source_cursor = source_conn.cursor()
        
        tables = get_all_tables(source_conn)
        if not tables:
            print("❌ 오류: 원본 DB에 테이블이 없습니다.")
            return

        print(f"\n✅ 원본 데이터베이스 '{source_db_name}'에 연결되었습니다.")
        print("\n--- [사용 가능한 테이블 목록] ---")
        for i, table in enumerate(tables, 1):
            print(f"{i}. {table}")

        # 3. 사용자로부터 추출할 테이블 이름 입력받기
        tables_input = input("\n추출할 테이블 이름을 입력하세요 (여러 개는 쉼표(,)로 구분): ")
        selected_tables = [t.strip() for t in tables_input.split(',')]

        # 입력된 테이블 이름 유효성 검사
        for t in selected_tables:
            if t not in tables:
                print(f"❌ 오류: '{t}' 테이블은 존재하지 않습니다. 프로그램을 종료합니다.")
                return
        
        # 4. 기존 결과 파일이 있으면 삭제
        if os.path.exists(output_db_name):
            os.remove(output_db_name)
            print(f"\n🧹 기존 '{output_db_name}' 파일을 삭제했습니다.")

        # 5. 새로운 DB에 연결하고 테이블 추출 시작
        dest_conn = sqlite3.connect(output_db_name)
        dest_cursor = dest_conn.cursor()
        print(f"\n🚀 테이블 추출을 시작합니다. -> '{output_db_name}'")

        for table_name in selected_tables:
            print(f"   - '{table_name}' 테이블 처리 중...")
            
            # (1) 원본 테이블의 구조(CREATE TABLE 문) 가져오기
            source_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            create_sql = source_cursor.fetchone()[0]
            
            # (2) 새로운 DB에 테이블 생성
            dest_cursor.execute(create_sql)
            
            # (3) 원본 테이블의 모든 데이터 가져오기
            source_cursor.execute(f'SELECT * FROM "{table_name}"')
            all_data = source_cursor.fetchall()
            
            # (4) 새로운 테이블에 모든 데이터 삽입
            if all_data:
                # 데이터의 컬럼 수에 맞춰서 '?' 플레이스홀더 생성
                placeholders = ', '.join(['?'] * len(all_data[0]))
                dest_cursor.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', all_data)
        
        # 6. 변경사항 저장
        dest_conn.commit()
        print("\n🎉 모든 작업이 완료되었습니다!")

    except sqlite3.Error as e:
        print(f"\n❌ 데이터베이스 오류가 발생했습니다: {e}")
    except Exception as e:
        print(f"\n❌ 알 수 없는 오류가 발생했습니다: {e}")
    finally:
        if source_conn:
            source_conn.close()
        if dest_conn:
            dest_conn.close()

if __name__ == "__main__":
    extract_tables()