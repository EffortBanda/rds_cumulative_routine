#!/bin/bash

source ~/.profile

atz=($(mysql -u$mysql_db_user -p$mysql_db_pwd -e "select SCHEMA_NAME  from information_schema.SCHEMATA s where lower(s.SCHEMA_NAME) like '%_sid_%'"))

for datz in ${atz[@]}
	                      do
			    dquery="drop schema if exists "$datz"";
			     mysql -u$mysql_db_user -p$mysql_db_pwd -e "$dquery"
			      done
