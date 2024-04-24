from pyfpa import fpa  # it does not support Pathlib object
import pandas as pd
import os
from pathlib import Path


# Path
input_path = Path(
    r"C:\Users\uid98421\Vitesco Technologies\Controlling VT Korea - Documents\110. Sales controlling\FY2024\080. SPOT"
)
output_path = Path(__file__).parent / "output"

# Filename
input_file = input_path / "SPOT 202401 ICH.xlsm"
# input_file = input_path / "SPOT 202402 ICH.xlsm"
# input_file = input_path / "SPOT 202403 ICH.xlsm"
output_file = output_path / "spot.project"

# Create instance
f = fpa()

# Read a single file
f.import_xl(
    str(input_file),
    ws_name="Monthly SPOT Overview",
    skiprows=9,
    cols_to_index=list(range(24)),
)
f.import_custom_xl(
    str(input_file),
    ws_name="Monthly SPOT Overview",
    skiprows=9,
    idx_cols=list(range(24)),
)


# Read multiple files
f.import_xl_directory(
    str(input_path),
    ws_name="Monthly SPOT Overview",
    skiprows=9,
    idx_cols=list(range(24)),
)
f.import_xl_directories(
    str(input_path),
    ws_name="Monthly SPOT Overview",
    skiprows=9,
    idx_cols=[0, 1, 2],
)

f.meta_block.to_clipboard()

# Save & Load project
f.save_project("SPOT_2024", str(output_path))
f.load_project(str(output_path / "SPOT_2024"))

# Consolidataion
f.consol_dimension()
