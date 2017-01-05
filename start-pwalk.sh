#! /bin/bash

PROGNAME=$(basename $0)

db_host="mydb"
database="storcrawldb"
#database="storcrawldb-dev"
db_user="storcrawl_user"
db_port=32048
#db_port=32050
db_conn_str="-h $db_host -p $db_port -d $database -U $db_user"

TS=$(date +%Y%m%d%H%M%S)
user_tbl="uid_mapping"
group_tbl="gid_mapping"
owner_tbl="folder_owners"
run_tbl="runlog"
file_metadata_tbl="file_metadata_$TS"
output_dir="output_$TS"
csv_dir="csv_$TS"

queuelength=15
folderlist="/fh/fast/_ADM/SciComp/benchmark/slurm-pwalk/bmcgough/fast-folders.txt"
#folderlist="/home/bmcgough/slurm-pwalk/test-folders.txt"
owner_csv_cmd="/fh/fast/_ADM/SciComp/benchmark/slurm-pwalk/bmcgough/treesize-folder-owners-to-csv.sh"
pwalk_worker="/fh/fast/_ADM/SciComp/benchmark/slurm-pwalk/bmcgough/pwalk-worker.sh"

function error_exit {
  echo "${PROGNAME}: $1" 1>&2
  exit 1
}

function load_psql {
  . /app/Lmod/lmod/lmod/init/bash
  module use /app/easybuild/modules/all
  module load PostgreSQL
}

function create_output_dirs {
  /app/bin/fhmkscratch $output_dir
  /app/bin/fhmkscratch $csv_dir
}

function drop_table {
  psql $db_conn_str -c "DROP TABLE IF EXISTS $1"
}

function drop_file_metadata_view {
  psql $db_conn_str -c "DROP VIEW file_metadata"
}

function create_run_table {
  psql $db_conn_str -c "CREATE TABLE $run_tbl(id serial, timestamp timestamp with time zone default now(),entry text not null,status boolean not null)"
}

function create_user_table {
  psql $db_conn_str -c "CREATE TABLE $user_tbl(name text,passwd text,uid bigint,gid bigint,gecos text,homedir text,shell text)"
}

function create_group_table {
  psql $db_conn_str -c "CREATE TABLE $group_tbl(name text,passwd text,gid bigint,members text)"
}

function create_owner_table {
  psql $db_conn_str -c "CREATE TABLE $owner_tbl(path text,owner text)"
}

function create_find_owner_function {
  psql $db_conn_str -c "CREATE OR REPLACE FUNCTION public.find_owner(_path text)
    RETURNS text AS $$
    DECLARE
      my_folder text;
      new_path text;
      trimmed_path text;
      my_owner text;
    BEGIN 
      IF _path = '/' THEN
        my_owner := '';
        my_folder := _path;
        RETURN my_owner;
      ELSE
        trimmed_path := trim(trailing '/' from _path);
        SELECT fo.owner INTO my_owner FROM folder_owners fo WHERE fo.path = trimmed_path;
        IF FOUND THEN
          RETURN my_owner;
        END IF;
        new_path := substring(trimmed_path,'(^.*/).*$');
        RETURN find_owner(new_path);
      END IF;
    END;
    $$
    LANGUAGE 'plpgsql' IMMUTABLE;"
}

function create_file_metadata_table {
  psql $db_conn_str -c "CREATE TABLE $file_metadata_tbl(id serial, inode bigint, parent_inode bigint, directory_depth int, filename text, fileExtension text, UID bigint, GID bigint, st_size bigint, st_blocks bigint, st_mode text, atime double precision, mtime double precision, ctime double precision, count int, sum bigint)"
}

function create_file_metadata_view {
  psql $db_conn_str -c "CREATE VIEW file_metadata AS SELECT * FROM $file_metadata_tbl"
}

# these are actually called
function sync_users {
  getent passwd | psql $db_conn_str -c "TRUNCATE TABLE $user_tbl; COPY $user_tbl(name,passwd,uid,gid,gecos,homedir,shell) FROM STDIN WITH DELIMITER ':'"
}

function sync_groups {
  getent group | psql $db_conn_str -c "TRUNCATE TABLE $group_tbl; COPY $group_tbl(name,passwd,gid,members) FROM STDIN WITH DELIMITER ':'"
}

function sync_owners {
  $owner_csv_cmd | psql $db_conn_str -c "COPY $owner_tbl(path,owner) FROM STDIN WITH csv"
  psql $db_conn_str -c "GRANT SELECT ON $owner_tbl TO storcrawl_ro"
}

function update_run_tbl {
  psql $db_conn_str -c "INSERT INTO $run_tbl(entry, status) values('$1',$2)"
}

function query_last_table_name {
  old_file_metadata_tbl=$(psql $db_conn_str -t -A -c "SELECT entry FROM $run_tbl ORDER BY timestamp DESC LIMIT 1")
}

function check_runlog_table {
  run_tbl_exists=$(psql $db_conn_str -t -A -c "SELECT EXISTS(SELECT * FROM information_schema.tables where table_name = '$run_tbl')")
  if [ "$run_tbl_exists" == "f" ]
  then
    create_run_table || echo "Unable to create runlog table!" 1>&2
  fi
}

# start pwalking
listsize=$(cat $folderlist | wc -l) || error_exit "Unable to read folderlist $folderlist!"
load_psql
echo "Checking run log table $run_tbl"
check_runlog_table || echo "Unable to check runlog table $run_tbl" 1>&2
echo "Creating output directories"
create_output_dirs || error_exit "Unable to create output dirs: $output_dir and $csv_dir!"
echo "Beginning pwalk for table $file_metadata_tbl"
echo -n "Checking on old table..."
query_last_table_name
if [ ! -z "$old_file_metadata_tbl" ]
then
  echo $old_file_metadata_tbl
fi
echo "Updating run log table for $file_metadata_tbl"
update_run_tbl $file_metadata_tbl TRUE || error_exit "Unable to update run log table!"
echo "Syncing uids..."
drop_table $user_tbl || error_exit "Unable to DROP user table!"
create_user_table || error_ext "Unable to CREATE user table!"
sync_users || echo "unsuccessful - uid mapping may be out of date!" 1>&2
echo "Syncing gids..."
drop_table $group_tbl || error_exit "Unable to DROP group table!"
create_group_table || error_ext "Unable to CREATE group table!"
sync_groups || echo "unsuccessful - gid mapping may be out of date!" 1>&2
echo "Syncing owners..."
drop_table $owner_tbl || error_exit "Unable to DROP owner table!"
create_owner_table || error_exit "Unable to CREATE owner table!"
sync_owners || echo "unsuccessful - owner data may be out of date!" 1>&2
echo "Creating file metadata table $file_metadata_tbl"
create_file_metadata_table || error_exit "Unable to CREATE files table!"
echo -n "Submitting job $file_metadata_tbl $listsize folders $queuelength jobs"
sbatch --array=0-${listsize}%${queuelength} --partition=boneyard \
       --mail-type=BEGIN,END,FAIL --mail-user=$(whoami) --time=1-0 \
       --output="$output_dir/output_%a_%A.%J.out" --cpus-per-task=12 \
       --job-name="pwalker" --requeue --wrap="$pwalk_worker $csv_dir $file_metadata_tbl $folderlist"

num_jobs=1
while [ $num_jobs -gt 0 ]
do
  sleep 60
  num_jobs=$(squeue -o "%A" -h -u ${USER} -n pwalker -S i | wc -l)
done

stop-pwalk.sh $file_metadata_tbl
