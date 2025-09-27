import sqlite3
import os
import logging
from typing import List, Optional, Set, Tuple

# 로깅 설정: 스크립트 진행 상황을 더 체계적으로 출력
logging.basicConfig(level=logging.INFO, format='%(message)s')

class DatabaseComparer:
    """
    두 개의 SQLite 데이터베이스를 비교하여 차이점을 세 번째 데이터베이스에 추출하는 클래스.

    주요 기능:
    - 두 DB의 공통 테이블에서 '신규 DB'에만 추가된 행을 찾습니다.
    - '신규 DB'에만 존재하는 새로운 테이블 전체를 추출합니다.
    - 결과를 '결과 DB'에 테이블 형태로 저장합니다.
    """
    # 비교를 위한 기준 키 컬럼 (우선순위 순서대로)
    KEY_COLUMNS = ['serverId_', 'updateTimestamp_']

    def __init__(self, old_db_path: str, new_db_path: str, result_db_path: str):
        self.old_db_path = old_db_path
        self.new_db_path = new_db_path
        self.result_db_path = result_db_path
        self.total_added_count = 0
        
        # 데이터베이스 연결 객체 초기화
        self.conn_old = None
        self.conn_new = None
        self.conn_result = None

    def __enter__(self):
        """'with' 구문 사용 시 DB 연결을 자동으로 엽니다."""
        self.conn_old = sqlite3.connect(self.old_db_path)
        self.conn_new = sqlite3.connect(self.new_db_path)
        self.conn_result = sqlite3.connect(self.result_db_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """'with' 구문 종료 시 DB 연결을 자동으로 닫습니다."""
        if self.conn_old: self.conn_old.close()
        if self.conn_new: self.conn_new.close()
        if self.conn_result: self.conn_result.close()

    def run_comparison(self):
        """데이터베이스 비교 및 추출 작업을 실행합니다."""
        logging.info("\n--- 2. 각 데이터베이스에서 테이블 목록 가져오는 중 ---")
        old_tables = self._get_table_names(self.conn_old)
        new_tables = self._get_table_names(self.conn_new)
        logging.info(f"'{self.old_db_path}'의 테이블: {list(old_tables)}")
        logging.info(f"'{self.new_db_path}'의 테이블: {list(new_tables)}")

        common_tables = sorted(list(old_tables.intersection(new_tables)))
        new_only_tables = sorted(list(new_tables - old_tables))

        if not common_tables and not new_only_tables:
            logging.warning("\n!!! 경고: 비교할 테이블이 하나도 없습니다. 작업을 종료합니다.")
            return self.total_added_count

        self._process_common_tables(common_tables)
        self._process_new_tables(new_only_tables)
        
        return self.total_added_count

    def _get_table_names(self, conn: sqlite3.Connection) -> Set[str]:
        """데이터베이스의 모든 테이블 이름을 집합(set)으로 가져옵니다."""
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return {table[0] for table in cursor.fetchall()}

    def _get_column_names(self, conn: sqlite3.Connection, table_name: str) -> List[str]:
        """특정 테이블의 모든 컬럼 이름을 리스트로 반환합니다."""
        cursor = conn.cursor()
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        return [row[1] for row in cursor.fetchall()]

    def _find_key_column(self, table_name: str) -> Optional[str]:
        """테이블에서 사용할 기준 키 컬럼을 찾습니다."""
        columns = self._get_column_names(self.conn_old, table_name)
        for key in self.KEY_COLUMNS:
            if key in columns:
                return key
        return None

    def _process_common_tables(self, tables: List[str]):
        """두 데이터베이스에 공통으로 존재하는 테이블들을 비교하고 처리합니다."""
        logging.info(f"\n--- 3. 공통 테이블 비교 시작 ---")
        logging.info(f"공통 테이블: {tables}")
        for table_name in tables:
            logging.info(f"\n▶ '{table_name}' 테이블 비교 중...")
            key_column = self._find_key_column(table_name)

            if not key_column:
                logging.warning(f"   !!! 경고: '{table_name}' 테이블에 기준 컬럼이 없어 건너뜁니다.")
                continue

            logging.info(f"   -> 기준 컬럼 '{key_column}'(으)로 비교를 시작합니다.")
            try:
                # 기존 DB와 신규 DB에서 키 값들을 가져옴
                cursor_old = self.conn_old.cursor()
                cursor_old.execute(f'SELECT "{key_column}" FROM "{table_name}"')
                ids_old = {row[0] for row in cursor_old.fetchall()}

                cursor_new = self.conn_new.cursor()
                cursor_new.execute(f'SELECT "{key_column}" FROM "{table_name}"')
                ids_new = {row[0] for row in cursor_new.fetchall()}

                # 신규 DB에만 있는 ID를 찾음
                added_ids = sorted(list(ids_new - ids_old))

                if not added_ids:
                    logging.info("   -> 추가된 데이터 없음")
                    continue

                logging.info(f"   -> {len(added_ids)}개의 추가된 데이터 발견.")
                self.total_added_count += len(added_ids)
                
                # 추가된 데이터를 결과 DB에 저장
                self._transfer_data(
                    source_conn=self.conn_new,
                    table_name=table_name,
                    filter_column=key_column,
                    filter_values=added_ids
                )
                logging.info(f"   -> '{table_name}' 테이블에 추가된 데이터 저장 완료")
            except sqlite3.Error as e:
                logging.error(f"   !!! '{table_name}' 테이블 처리 중 데이터베이스 오류 발생: {e}")

    def _process_new_tables(self, tables: List[str]):
        """신규 데이터베이스에만 존재하는 테이블 전체를 복사합니다."""
        logging.info("\n--- 4. 신규 DB에만 존재하는 테이블 처리 ---")
        if not tables:
            logging.info("ℹ️ 새로 추가된 테이블은 없습니다.")
            return
            
        logging.info(f"✨ 새로 추가된 테이블 발견: {tables}")
        for table_name in tables:
            logging.info(f"\n▶ '{table_name}' 테이블 전체 데이터 추출 중...")
            try:
                # 테이블의 모든 데이터를 결과 DB에 저장
                count = self._transfer_data(self.conn_new, table_name)
                if count > 0:
                    self.total_added_count += count
                    logging.info(f"   -> '{table_name}' 테이블에 {count}개의 데이터를 저장 완료했습니다.")
                else:
                    logging.info("   -> 테이블은 있으나 데이터가 없어 건너뜁니다.")

            except sqlite3.Error as e:
                logging.error(f"   !!! '{table_name}' 테이블 처리 중 오류 발생: {e}")

    def _transfer_data(self, source_conn: sqlite3.Connection, table_name: str, 
                         filter_column: Optional[str] = None, filter_values: Optional[List] = None) -> int:
        """소스 DB에서 데이터를 읽어 결과 DB에 저장하고, 저장된 행의 수를 반환합니다."""
        source_cursor = source_conn.cursor()
        
        query = f'SELECT * FROM "{table_name}"'
        params = []
        if filter_column and filter_values:
            # SQL Injection 방지를 위해 '?' placeholder 사용
            placeholders = ",".join("?" for _ in filter_values)
            query += f' WHERE "{filter_column}" IN ({placeholders})'
            params = filter_values

        source_cursor.execute(query, params)
        data_to_transfer = source_cursor.fetchall()

        if not data_to_transfer:
            return 0
            
        # 테이블 스키마(CREATE 문)를 가져와 결과 DB에 테이블 생성
        source_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        create_table_sql = source_cursor.fetchone()[0]
        
        result_cursor = self.conn_result.cursor()
        result_cursor.execute(f"DROP TABLE IF EXISTS \"{table_name}\"")
        result_cursor.execute(create_table_sql)

        # 데이터 삽입
        placeholders = ', '.join(['?'] * len(data_to_transfer[0]))
        result_cursor.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', data_to_transfer)
        self.conn_result.commit()
        
        return len(data_to_transfer)


def find_db_files(directory: str = '.') -> Optional[Tuple[str, str]]:
    """현재 디렉토리에서 비교할 SQLite DB 파일 두 개를 찾습니다."""
    logging.info(f"--- 1. 현재 폴더({os.path.abspath(directory)})에서 파일 찾는 중 ---")
    try:
        all_files = os.listdir(directory)
        db_files = [f for f in all_files if f.endswith(('.db', '.sqlite')) and f != '추가목록.db']

        if len(db_files) < 2:
            logging.error("\n!!! 오류: 비교할 DB 파일이 2개 미만입니다. 폴더에 .db 또는 .sqlite 파일 2개가 있는지 확인해 주세요.")
            return None
        
        # 파일 수정 시간을 기준으로 정렬하여 이전/최신 DB를 구분
        db_files.sort(key=lambda f: os.path.getmtime(os.path.join(directory, f)))
        
        old_db, new_db = db_files[0], db_files[1]
        logging.info("성공: DB 파일 2개를 찾았습니다.")
        logging.info(f"'{old_db}' (기존 DB)와 '{new_db}' (신규 DB)를 비교합니다.")
        return old_db, new_db

    except FileNotFoundError:
        logging.error(f"!!! 오류: '{directory}' 폴더를 찾을 수 없습니다.")
        return None


def main():
    """스크립트의 메인 실행 함수입니다."""
    db_paths = find_db_files()
    
    if db_paths:
        old_db, new_db = db_paths
        output_db = '추가목록.db'
        
        if os.path.exists(output_db):
            os.remove(output_db)
            logging.info(f"\n🧹 기존 '{output_db}' 파일을 삭제했습니다. 새로운 결과로 교체됩니다.")

        try:
            # with 구문을 사용하여 DatabaseComparer 객체 생성 및 실행
            with DatabaseComparer(old_db, new_db, output_db) as comparer:
                total_added = comparer.run_comparison()

            logging.info("\n--- 5. 모든 작업 완료 ---\n")
            logging.info("---  최종 결과 요약 ---")
            if total_added > 0:
                logging.info(f"🎉 총 {total_added}개의 새로운 데이터가 발견되어 '{output_db}'에 저장되었습니다.")
            else:
                logging.info("ℹ️ 비교 결과, 추가된 데이터가 없습니다.")

        except Exception as e:
            logging.error(f"스크립트 실행 중 예기치 않은 오류 발생: {e}")

if __name__ == '__main__':
    main()