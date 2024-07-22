import win32com.client
from pathlib import Path


# Path
path = Path(__file__).parent.parent


# Functions
def main():

    # Path
    data_path = path / "data" / "2024-06" / "Profit and Loss"

    # Filenames
    output_file = path / "output" / "2024-06_P&L AP.xlsx"

    # Input data: List of multiple text files
    xls_files = [
        file
        for file in data_path.iterdir()
        if file.is_file() and file.suffix == ".xlsx"
    ]

    # win32com(pywin32)를 이용해서 엑셀 어플리케이션 열기
    excel = win32com.client.Dispatch("Excel.Application")
    excel.Visible = False  # 실제 작동하는 것을 보고 싶을 때 사용

    # step3.엑셀 어플리케이션에 새로운 Workbook 추가
    wb_new = excel.Workbooks.Add()

    # step5.엑셀 시트를 추출하고 새로운 엑셀에 붙여넣는 반복문
    for file in xls_files:

        # 받아온 엑셀 파일의 경로를 이용해 엑셀 파일 열기
        wb = excel.Workbooks.Open(file)

        # 새로 만든 엑셀 파일에 추가
        # 추출할wb.Worksheets("추출할 시트명").Copy(Before=붙여넣을 wb.Worksheets("기준 시트명")
        wb.Worksheets("Report").Copy(Before=wb_new.Worksheets("Sheet1"))

    # Write data
    wb_new.SaveAs(str(output_file))
    print("A file is created")

    # 켜져있는 엑셀 및 어플리케이션 모두 종료
    excel.Quit()


if __name__ == "__main__":
    main()
