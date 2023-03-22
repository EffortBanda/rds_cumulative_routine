#!/bin/bash
#
#
#Bash script to prepare temporary scripts for each rds schema for all POC facilities and load them in parallel on their respective schemas.    
#
#
#Variable declarations

source ~/.profile

query="select s.SCHEMA_NAME  from information_schema.SCHEMATA s where lower(s.SCHEMA_NAME) like '%_sid_%'";
rds_schema=($(mysql -u$mysql_db_user -p$mysql_db_pwd -Dinformation_schema -e "$query"))
cd /home/cdr-user/CDR/dumps/rds/quarterly_dumps/loaded/
mv ./temp/rds*.gz ./loaded 
mv ./rds*.gz ./temp
rm ./missing_dumps.text

for schema in "${rds_schema[@]}"
 do
  dump_counter=0
  rm ./temp/"$schema"_load.sh
  echo "Checking for full dumps or deltas for schema : $schema"
  sid=$(echo $schema |grep -Eo '[0-9]+$')
  echo "#!/bin/bash" >> ./temp/"$schema"_load.sh
  echo "source ~/.profile" >> ./temp/"$schema"_load.sh
  echo "pwd=\$mysql_db_pwd" >> ./temp/"$schema"_load.sh
  echo "{" >> ./temp/"$schema"_load.sh

  for dump in $(ls -tr ./temp/rds*.gz)
   do
     did=$(zgrep -m 4 -o -P '.{0,0}VALUES.{0,20}' "$dump" | cut -c 9- | sed 's/,/\n/1;P;d' | egrep -o '.{1,5}$' | uniq | sed 's/^0*//')
     if [[ " ${did} " -eq " ${sid} " ]]; then
         echo "Including $dump in script for $schema"
         echo  "zcat $dump | mysql -u\$mysql_db_user -p\$mysql_db_pwd $schema" >> ./temp/"$schema"_load.sh
         ((dump_counter=dump_counter+1))
     else
         echo "skipping $dump because it is not associated with $schema"
     fi
    done

  echo "} &> /dev/null" >> ./temp/"$schema"_load.sh

  if [[ "$dump_counter" -gt 0 ]]; then
   chmod u+x ./temp/"$schema"_load.sh
   ./temp/"$schema"_load.sh &
  else
    echo "dumps missing for $schema . Verify to see if there is an issue" >> ./missing_dumps.text
    rm ./temp/"$schema"_load.sh
  fi
done

