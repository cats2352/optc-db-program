import sqlite3
import os
import pyperclip

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

def main():
    """메인 로직을 실행합니다."""
    db_file = find_db_file()
    if not db_file:
        return

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"\n✅ 데이터베이스 '{db_file}'에 정상적으로 연결되었습니다.")
        cursor = conn.cursor()

        # --- 1단계: 테이블 선택 ---
        tables = get_all_tables(cursor)
        if not tables:
            print("❌ 오류: 데이터베이스에 테이블이 없습니다.")
            return
            
        print("\n--- [사용 가능한 테이블 목록] ---")
        for i, table in enumerate(tables, 1):
            print(f"{i}. {table}")
        
        table_name = input("\n[1단계] 위 목록에서 데이터를 추출할 테이블 이름을 입력하세요: ")
        
        if table_name not in tables:
            print(f"❌ 오류: '{table_name}' 테이블을 찾을 수 없습니다.")
            return

        # --- 2단계: 컬럼 선택 ---
        columns = get_all_columns(cursor, table_name)
        if not columns:
            print(f"❌ 오류: '{table_name}' 테이블에 컬럼이 없습니다.")
            return

        print(f"\n--- ['{table_name}' 테이블의 컬럼 목록] ---")
        for i, col in enumerate(columns, 1):
            print(f"{i}. {col}")

        columns_input = input(f"\n[2단계] 추출할 컬럼을 입력하세요 (여러 개는 쉼표(,), 전체는 * 입력): ")
        
        columns_to_extract = []
        if columns_input.strip() == '*':
            columns_to_extract = columns
        else:
            columns_to_extract = [col.strip() for col in columns_input.split(',')]
            for col in columns_to_extract:
                if col not in columns:
                    print(f"❌ 오류: '{table_name}' 테이블에 '{col}' 컬럼이 없습니다.")
                    return

        # --- 3단계 (신규): WHERE 조건 입력받기 ---
        print("\n[3단계] 데이터 필터링 조건을 입력하세요 (없으면 그냥 엔터)")
        where_clause = input(" (예: rarity_ = 5 또는 name_ LIKE '%루피%'): ")

        # --- 4단계: 데이터 조회 및 추출 ---
        columns_for_query = ', '.join([f'"{col}"' for col in columns_to_extract])
        query = f'SELECT {columns_for_query} FROM "{table_name}"'
        
        # 사용자가 WHERE 조건을 입력했다면 쿼리에 추가
        if where_clause.strip():
            query += f" WHERE {where_clause.strip()}"

        print(f"\n실행할 쿼리: {query}")
        
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            print("\n결과 없음: 해당 조건에 맞는 데이터가 없습니다.")
            return

        # --- 결과 포맷팅 및 복사 ---
        all_output_lines = []
        if len(columns_to_extract) > 1:
            for i, row in enumerate(results):
                formatted_row = "\n".join(map(str, row))
                all_output_lines.append(formatted_row)
                if i < len(results) - 1:
                    all_output_lines.append("---")
        else:
            for row in results:
                all_output_lines.append(str(row[0]))

        final_output = "\n".join(all_output_lines)

        print("\n--- 📋 추출 완료 ---")
        print(f"총 {len(results)}개의 행(row) 데이터가 추출되었습니다.")
        pyperclip.copy(final_output)
        print("결과가 클립보드에 복사되었습니다!")
        
        print("\n--- 미리보기 ---")
        print(final_output[:500])
        if len(final_output) > 500:
            print("...")

    except sqlite3.Error as e:
        print(f"\n❌ 데이터베이스 오류가 발생했습니다: {e}")
    except Exception as e:
        print(f"\n❌ 알 수 없는 오류가 발생했습니다: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()