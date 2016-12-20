#! /bin/bash

csv_dir=$1
file_metadata_tbl=$2
folderlist=$3

db_host="mydb"
database="storcrawldb"
#database="storcrawldb-dev"
db_user="storcrawl_user"
db_port=32048
#db_port=32050
db_conn_str="-h $db_host -p $db_port -d $database -U $db_user"

if [[ -z $SLURM_ARRAY_TASK_ID ]]; then
  echo "you need to run this script as a slurm array job"
  exit 1
fi

# read folder list into array
declare -a myArray
mapfile -t myArray < $folderlist

/app/bin/pwalk --NoSnap  --maxthreads=32 \
      /fh/fast/${myArray[$SLURM_ARRAY_TASK_ID]} \
    > $csv_dir/${myArray[$SLURM_ARRAY_TASK_ID]}.csv

. /app/Lmod/lmod/lmod/init/bash
module use /app/easybuild/modules/all
module load PostgreSQL

cat $csv_dir/${myArray[$SLURM_ARRAY_TASK_ID]}.csv | uconv -s -i | psql $db_conn_str -c "COPY $file_metadata_tbl(inode,parent_inode,directory_depth,filename,fileextension,uid,gid,st_size,st_blocks,st_mode,atime,mtime,ctime,count,sum) FROM STDIN WITH csv ESCAPE '\'"
