# Convert from Excel to CSV

in2csv -f xlsx Monthly\ SPOT\ Overview_Icheon_202205.xlsm > SPOT_202205.csv
in2csv -f xlsx Monthly\ SPOT\ Overview_Icheon_202206.xlsm > SPOT_202206.csv
in2csv -f xlsx Monthly\ SPOT\ Overview_Icheon_202207.xlsm > SPOT_202207.csv
in2csv -f xlsx Monthly\ SPOT\ Overview_Icheon_202208.xlsm > SPOT_202208.csv
in2csv -f xlsx Monthly\ SPOT\ Overview_Icheon_202209.xlsm > SPOT_202209.csv
in2csv -f xlsx Monthly\ SPOT\ Overview_Icheon_202210.xlsm > SPOT_202210.csv
in2csv -f xlsx Monthly\ SPOT\ Overview_Icheon_202211.xlsm > SPOT_202211.csv

# Filter out blank lines in 1st column
csvgrep -c 1 -r "^$" -i SPOT_202205.csv > SPOT_202205_.csv
csvgrep -c 1 -r "^$" -i SPOT_202206.csv > SPOT_202206_.csv
csvgrep -c 1 -r "^$" -i SPOT_202207.csv > SPOT_202207_.csv
csvgrep -c 1 -r "^$" -i SPOT_202208.csv > SPOT_202208_.csv
csvgrep -c 1 -r "^$" -i SPOT_202209.csv > SPOT_202209_.csv
csvgrep -c 1 -r "^$" -i SPOT_202210.csv > SPOT_202210_.csv
csvgrep -c 1 -r "^$" -i SPOT_202211.csv > SPOT_202211_.csv

# Filter out 1st lines with "qq"
grep qq -v SPOT_202205_.csv > SPOT_202205.csv
grep qq -v SPOT_202206_.csv > SPOT_202206.csv
grep qq -v SPOT_202207_.csv > SPOT_202207.csv
grep qq -v SPOT_202208_.csv > SPOT_202208.csv
grep qq -v SPOT_202209_.csv > SPOT_202209.csv
grep qq -v SPOT_202210_.csv > SPOT_202210.csv
grep qq -v SPOT_202211_.csv > SPOT_202211.csv

# Insert text at the beginning of the line
sed -i 's/^/202205,/' SPOT_202205.csv
sed -i 's/^/202206,/' SPOT_202206.csv
sed -i 's/^/202207,/' SPOT_202207.csv
sed -i 's/^/202208,/' SPOT_202208.csv
sed -i 's/^/202209,/' SPOT_202209.csv
sed -i 's/^/202210,/' SPOT_202210.csv
sed -i 's/^/202211,/' SPOT_202211.csv


# Combile files
cat SPOT_2022*.csv > SPOT_combined.csv