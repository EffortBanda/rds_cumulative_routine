#!/bin/bash
#
#
#Bash script to prepare temporary scripts for each rds schema for all POC facilities and load them in parallel on their respective schemas.    
#
#
#Variable declarations
source ~/.profile


#Free up resources for execution of background processes
#Stop db service

echo $server_pwd | sudo -S service postgresql stop
echo $server_pwd | sudo -S service mysql stop

#Clear PageCache only

echo $server_pwd | sudo -S sh -c "echo 1 > /proc/sys/vm/drop_caches"

#Clear dentries and inodes

echo $server_pwd | sudo -S sh -c "echo 2 > /proc/sys/vm/drop_caches"

#Clear pagecache, dentries, and inodes

echo $server_pwd | sudo -S sh -c "echo 3 > /proc/sys/vm/drop_caches"

#start db service

echo $server_pwd | sudo -S service postgresql start
echo $server_pwd | sudo -S service mysql start


query="select s.SCHEMA_NAME  from information_schema.SCHEMATA s where lower(s.SCHEMA_NAME) like '%_sid_%'";
rds_schema=($(mysql -u$mysql_db_user -p$mysql_db_pwd -Dinformation_schema -e "$query"))
unset rds_schema[0]
cd /home/cdr-user/CDR/dumps/rds/quarterly_dumps/loaded/
mv ./temp/rds*.gz ./loaded 
mv ./rds*.gz ./temp
rm ./missing_dumps.text

bgarray=()
active_schema=()
for schema in "${rds_schema[@]}"
 do
  dump_counter=0
  rm ./temp/"$schema"_load.sh
  echo "Checking for full dumps or deltas for schema : $schema"
  sid=$(echo $schema | grep -Eo '[0-9]+$')
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
    pid=$!
    bgarray+=($pid)
    active_schema+=("${schema}")
  else
    echo "dumps missing for $schema . Verify to see if there is an issue" >> ./missing_dumps.text
    rm ./temp/"$schema"_load.sh
  fi
done

 echo "Batch is ready for processing"
 echo "The following background processes for dump restoration have been started:"
 printf "\n"
 echo  ${bgarray[@]}
 printf "\n"

echo "The following dumps will go through ETL after the restoration is complete"
 printf "\n"
 echo  ${active_schema[@]}
 printf "\n"

 for job in ${bgarray[@]}
    do
     ps -ho pid | >/dev/null grep $job
     echo "[Background process for dump restoration with id $job is still running. waiting for completion of this task and others..]"
     wait $job
    done



#Free up resources for execution of ETL processes
#Stop db service

echo $server_pwd | sudo -S service postgresql stop
echo $server_pwd | sudo -S service mysql stop

#Clear PageCache only

echo $server_pwd | sudo -S sh -c "echo 1 > /proc/sys/vm/drop_caches"

#Clear dentries and inodes

echo $server_pwd | sudo -S sh -c "echo 2 > /proc/sys/vm/drop_caches"

#Clear pagecache, dentries, and inodes

echo $server_pwd | sudo -S sh -c "echo 3 > /proc/sys/vm/drop_caches"

#start db service

echo $server_pwd | sudo -S service postgresql start
echo $server_pwd | sudo -S service mysql start





for sc in ${active_schema[@]}
do
         echo "STARTING PROCEDURE FOR RDS SCHEMA : $sc "
          loaded=$(mysql -u$mysql_db_user -p$mysql_db_pwd -se "select count(*)  from $sc.encounter ")
           if [ $loaded -gt 1000 ]; then

                     db="$sc"
                       cd '/opt/pentaho/CDWRepository'
                         sed -i -e "s/\(<database>\).*\(<\/database>\)/<database>${db}<\/database>/g" RDS.kdb
                           cd '/opt/pentaho'
                             export KETTLE_HOME=/opt/pentaho
                               ./CDWRun.sh

                                else
                                          echo "Skipping $sc because it does not have encounters implying that there is an issue or it was not loaded"
                                           fi

                                   done
