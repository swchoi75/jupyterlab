import pandas as pd
import ibis
from ibis import selectors as s
from ibis import _
from pathlib import Path

# ibis.options.interactive = True


# Path
try:
    path = Path(__file__).parent.parent.parent
except NameError:
    import inspect

    path = Path(inspect.getfile(lambda: None)).resolve().parent.parent.parent


def read_data(filename):
    df = pd.read_csv(
        filename,
        dtype={
            "m_y_from_": str,
            "sap_plant": str,
            "outlet": str,
            "vendor": str,
            "trading_pr": str,
            "accounts_f": str,
            "document_d": str,
        },
    )
    return df


def main():

    # Filenames
    input_file = path / "db" / "ZMPV_2024.csv"
    output_1 = path / "output" / "ZMPV ICO purchase.csv"
    output_2 = path / "output" / "ZMPV Net PPV.csv"
    output_3 = path / "output" / "ZMPV FX Material.csv"
    output_4 = path / "output" / "ZMPV STD LOCO.csv"
    output_5 = path / "output" / "ZMPV SMD outsourcing.csv"

    # Read data
    df = read_data(input_file).drop(columns=["document_d"])  # only Null value
    zmpv = ibis.memtable(df)

    # Process data
    zmpv_ico = zmpv.filter(_.outs_ic == "IC")
    zmpv_ppv = zmpv.filter((_.net_pm_ppv < -10000) | (_.net_pm_ppv > 10000))
    zmpv_fx = zmpv.filter((_.fx_effect < -10000) | (_.fx_effect > 10000))
    zmpv_loco = zmpv.filter(
        _.std_tool_c.abs() + _.std_freigh.abs() + _.std_customs.abs() > 10000
    )
    zmpv_smd = zmpv.filter(
        (_.vendor == "0009085884") | (_.vendor == "0009072686")
    ).filter(_.gr_quantity != ibis.NA)

    # Write CSV files
    zmpv_ico.to_pandas().to_csv(output_1, index=False, na_rep="0")
    zmpv_ppv.to_pandas().to_csv(output_2, index=False, na_rep="0")
    zmpv_fx.to_pandas().to_csv(output_3, index=False, na_rep="0")
    zmpv_loco.to_pandas().to_csv(output_4, index=False, na_rep="0")
    zmpv_smd.to_pandas().to_csv(output_5, index=False, na_rep="0")
    print("Files are created")


if __name__ == "__main__":
    main()
