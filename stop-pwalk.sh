#! /bin/bash

PROGNAME=$(basename $0)

file_metadata_tbl=$1

db_host="mydb"
database="storcrawldb"
#database="storcrawldb-dev"
db_user="storcrawl_user"
db_port=32048
#db_port=32050
db_conn_str="-h $db_host -p $db_port -d $database -U $db_user"

run_tbl="runlog"

# keep the last n file_metadata crawl tables
keep_crawls=5

function error_exit {
  echo "${PROGNAME}: $1" 1>&2
  exit 1
}

function load_psql {
  . /app/Lmod/lmod/lmod/init/bash
  module use /app/easybuild/modules/all
  module load PostgreSQL
}

function drop_table {
  psql $db_conn_str -c "DROP TABLE IF EXISTS $1"
}

function drop_file_metadata_view {
  psql $db_conn_str -c "DROP VIEW IF EXISTS file_metadata"
}

function create_file_metadata_view {
  psql $db_conn_str -c "CREATE VIEW file_metadata AS SELECT * FROM $file_metadata_tbl"
}

function grant_ro_table {
  psql $db_conn_str -c "GRANT SELECT ON $1 TO storcrawl_ro"
}

function grant_ro_view {
  psql $db_conn_str -c "GRANT SELECT ON file_metadata TO storcrawl_ro"
}

function update_run_tbl {
  psql $db_conn_str -c "INSERT INTO $run_tbl(entry, status) values('$1',$2)"
}

function clean_tables {
  table_list=$(psql $db_conn_str -t -c "select table_name from information_schema.tables where table_name like 'file_metadata_%' order by table_name desc")

  for t in $table_list
  do
    if [ "$keep_crawls" == 0 ]
    then
      drop_table $t
    else
      keep_crawls=$((keep_crawls - 1))
    fi
  done
}

# stop pwalking
load_psql
echo "Ending pwalk for table $file_metadata_tbl"
echo "Updating run log table for $file_metadata_tbl"
update_run_tbl $file_metadata_tbl FALSE || echo "Unable to update run log table!" 1>&2
echo "Updating file_metadata view"
drop_file_metadata_view || echo "Unable to drop file_metadata view" 1>&2
create_file_metadata_view || echo "Unable to create file_metadata view" 1>&2
echo "Granting ro access"
grant_ro_table $file_metadata_tbl
grant_ro_view
echo "Removing old tables"
clean_tables
