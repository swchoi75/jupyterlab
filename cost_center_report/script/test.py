from pyfpa import fpa
from pathlib import Path


# Path
try:
    path = Path(__file__).parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent


# Filename
input_1 = path / "data" / "sales1.xlsx"
input_2 = path / "data" / "sales2.xlsx"


# Read data
f = fpa()
f.import_xl(str(input_1))

f.block
f.move_col_to_dims(["name", "city"], "block")
f.add_block_to_data()
f.data

f.import_xl(str(input_2), cols_to_index=[0, 1, 2])


# Slice data
f.slice_data(["city"], [["chicago", "rockland"]], col_list=["units"])
f.slice
f.slice.to_clipboard()
