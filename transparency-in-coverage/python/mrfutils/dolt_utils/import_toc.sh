for table in toc toc_plan toc_file toc_plan_file;
do
  echo WRITING TABLE $table
  dolt table import -u $table $1/$table.csv
done
