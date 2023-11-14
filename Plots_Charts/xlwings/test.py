import pandas as pd
import xlwings as xw


df = pd.read_csv("euromillions.csv")
print(df.sample(5))

# wb = xw.Book(filename) would open an existing file
wb = xw.Book()

# creates a worksheet object assigns it to ws
ws = wb.sheets["Sheet1"]

# checks that wb.sheets[0] equals ws
print(ws == wb.sheets[0])

ws.name = "EuroMillions"

# ws.range("A1") is a Range object
ws.range("A1").value = df

# Without Index
ws.clear_contents()
ws.range("A1").options(index=False).value = df

# Row
last_row = ws.range(1, 1).end("down").row
print("The last row is {row}.".format(row=last_row))
print("The DataFrame df has {rows} rows.".format(rows=df.shape[0]))

# # API Sort - missing feature
# # following code not working
# ws.range(
# "A2:N{row}".format(row=last_row)
# ).api.Sort(Key1=ws.range("A:A").api, Order1=1)

# Header
ws.range("O1").value = "Date"

# Formula
ws.range("O2").value = '=C2&" "&D2&" "&RIGHT(E2, 2)'

# API Autofill
ws.range("O2").api.AutoFill(ws.range("O2:O{row}".format(row=last_row)).api, 0)

from xlwings.constants import AutoFillType

ws.range("O2").api.AutoFill(
    ws.range("O2:O{row}".format(row=last_row)).api, AutoFillType.xlFillDefault
)
