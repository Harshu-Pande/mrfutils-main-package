# Convenience script for importing the table data in the right order
# Usage: bash import_in_network.sh <out_dir> 
# <out_dir> is where your root.csv (etc.) files are saved

for table in file code rate_metadata rate tin tin_rate_file npi_tin;
do
  echo WRITING TABLE $table
  dolt table import -u $table $1/$table.csv
done
