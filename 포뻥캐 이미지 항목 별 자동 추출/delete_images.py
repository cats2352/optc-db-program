import os
import tkinter as tk
from tkinter import messagebox

def delete_extracted_images():
    """
    스크립트가 있는 위치의 '추출된_이미지' 폴더 안의 모든 파일을 삭제합니다.
    """
    try:
        # 1. 현재 스크립트가 있는 폴더의 경로를 가져옵니다.
        current_folder = os.path.dirname(os.path.abspath(__file__))
        # 2. 삭제할 대상 폴더의 경로를 설정합니다.
        target_folder = os.path.join(current_folder, "추출된_이미지")

        # 3. '추출된_이미지' 폴더가 존재하는지 확인합니다.
        if not os.path.exists(target_folder):
            messagebox.showinfo("알림", "'추출된_이미지' 폴더를 찾을 수 없습니다.\n삭제할 파일이 없습니다.")
            return

        # 4. 대상 폴더 안에 있는 모든 파일의 목록을 가져옵니다.
        file_list = os.listdir(target_folder)

        if not file_list:
            messagebox.showinfo("알림", "'추출된_이미지' 폴더가 비어있습니다.")
            return

        deleted_count = 0
        # 5. 목록에 있는 모든 파일을 하나씩 삭제합니다.
        for filename in file_list:
            file_path = os.path.join(target_folder, filename)
            # 파일만 삭제하도록 확인 (하위 폴더가 있을 경우를 대비)
            if os.path.isfile(file_path):
                os.remove(file_path)
                deleted_count += 1
        
        messagebox.showinfo("삭제 완료", f"총 {deleted_count}개의 파일을 '추출된_이미지' 폴더에서 삭제했습니다.")

    except Exception as e:
        messagebox.showerror("오류 발생", f"파일 삭제 중 오류가 발생했습니다:\n{e}")


# 메인 GUI 창을 숨기기 위해 설정
root = tk.Tk()
root.withdraw()

# 사용자에게 정말 삭제할 것인지 확인을 받습니다.
if messagebox.askyesno("삭제 확인", "'추출된_이미지' 폴더 안의 모든 파일을 정말로 삭제하시겠습니까?\n이 작업은 되돌릴 수 없습니다."):
    delete_extracted_images()
else:
    messagebox.showinfo("작업 취소", "파일 삭제 작업이 취소되었습니다.")