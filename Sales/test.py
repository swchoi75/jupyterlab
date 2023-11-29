import pandas as pd


# Test
ph_file = path / "meta" / "product_hierarchy_BU CT.csv"
ph = pd.read_csv(ph_file, encoding="utf-8-sig")
ph.head()
ph.info()
