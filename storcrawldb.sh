#! /bin/bash

PROGNAME=$(basename $0)

## source config
source ./storcrawldb.config

## source functions
source $storcrawldb_db_functions
source $storcrawldb_functions

# check arguments
while [[ $# > 1 ]]
do
case $1 in
  --action)
  storcrawl_action="$2"
  shift
  ;;
  --storcrawl_config)
  storcrawl_config="$2"
  shift
  ;;
  --tag)
  specified_tag="$2"
  shift
  ;;
  --force)
  forced="$2"
  shift
  ;;
  *)
  usage
  ;;
esac
shift
done

## generate tag
if [ -z "$specified_tag" ]
then
  specified_tag=$(date +%Y%m%d%H%M)
fi

export STORCRAWLDB_TAG=${STORCRAWLDB_TAG:=$specified_tag}
export STORCRAWLDB_START_PATH=${STORCRAWLDB_START_PATHS:=$start_paths}
export STORCRAWLDB_FILE_TABLE=${STORCRAWLDB_FILE_TABLE:="${file_tbl}${STORCRAWLDB_TAG}"}
export STORCRAWLDB_FOLDER_TABLE=${STORCRAWLDB_FOLDER_TABLE:="${folder_tbl}${STORCRAWLDB_TAG}"}
export STORCRAWLDB_LOG_TABLE=${STORCRAWLDB_LOG_TABLE:="${log_tbl}${STORCRAWLDB_TAG}"}
export STORCRAWLDB_SOURCE_TABLE=${STORCRAWLDB_SOURCE_TABLE:="${source_tbl}${STORCRAWLDB_TAG}"}
export STORCRAWLDB_REPORT_TABLE=${STORCRAWLDB_REPORT_TABLE:="${report_tbl}${STORCRAWLDB_TAG}"}
export STORCRAWLDB_CSV_DIR=${STORCRAWLDB_CSV_DIR:="${csv_dir}${STORCRAWLDB_TAG}"}
export STORCRAWLDB_OUTPUT_DIR=${STORCRAWLDB_OUTPUT_DIR:="${output_dir}${STORCRAWLDB_TAG}"}
export STORCRAWLDB_VIEW=${STORCRAWLDB_VIEW:=$storcrawldb_view}

## load PostgreSQL Environment Modules
source ${lmod_init}
module use ${modulefile_dir}
module load ${psql_module}

## basic checking
#  exists: output_dir, csv_dir, scripts dirs, start_path
#  db connection
#  slurm commands

function storcrawl_start {
  # initialize db
  init_storcrawldb
  # check for tag
  check_tag
  # check that last crawl finished
  check_last_crawl
  # init the crawl tables
  init_crawl
  storcrawl_log $FUNCNAME
  # build the folder table
  build_folder_table
  get_job_array_size
  # run before scripts
  run_scripts "${before_script_dir}"
  # check output dirs
  if [ ! -d "$STORCRAWLDB_CSV_DIR" ]
  then
    mkdir -p $STORCRAWL_CSV_DIR ||\
      error_exit "Unable to create CSV dir $STORCRAWLDB_CSV_DIR"
  fi
  if [ ! -w "$STORCRAWLDB_CSV_DIR" ]
  then
    error_exit "Unable to write to CSV dir $STORCRAWLDB_CSV_DIR"
  fi
  if [ ! -d "$STORCRAWLDB_OUTPUT_DIR" ]
  then
    mkdir -p $STORCRAWL_OUTPUT_DIR ||\
      error_exit "Unable to create OUTPUT dir $STORCRAWLDB_OUTPUT_DIR"
  fi
  if [ ! -w "$STORCRAWLDB_OUTPUT_DIR" ]
  then
    error_exit "Unable to write to OUTPUT dir $STORCRAWLDB_OUTPUT_DIR"
  fi
  # run crawl batch
  sbatch_job_name="${sbatch_job_name_prefix}${STORCRAWLDB_TAG}"
  storcrawl_log "scheduling crawl jobs"
  echo "sbatch maxarraysize is ${sbatch_maxarraysize}"
  num_job_arrays=$((${STORCRAWLDB_JOB_ARRAY_SIZE}/${sbatch_maxarraysize}+1))
  job_array_end_id="${sbatch_maxarraysize}"
  for i in $(seq 1 $num_job_arrays)
  do
    if [ "$i" -eq "$num_job_arrays" ]
    then
      # the last job array batch
      job_array_end_id=$((${STORCRAWLDB_JOB_ARRAY_SIZE}%${sbatch_maxarraysize}))
    fi
    jobid=$(sbatch --array="1-${job_array_end_id}%${sbatch_simultaneous_tasks}" \
                   --partition="$sbatch_partition" \
                   --mail-type="$sbatch_mail_type" \
                   --mail-user="$sbatch_mail_user" \
                   --time="$sbatch_time" \
                   --cpus-per-task="$sbatch_cpus_per_task" \
                   --job-name="$sbatch_job_name" \
                   --output="${STORCRAWLDB_OUTPUT_DIR}/output_%a_%A.%J.out" \
                   --wrap="$0 --action crawl")
    if [ $? -eq 0 ]
    then
      echo "crawl array job launched - jobid ${jobid}"
    else
      error_exit "Ooops, unable to launch crawl jobs, exiting..."
    fi
  done

  # run cleanup job
  storcrawl_log "scheduling cleanup job"
  jobid=$(sbatch --partition="$sbatch_partition" \
                 --mail-type="$sbatch_mail_type" \
                 --mail-user="$sbatch_mail_user" \
                 --time="$sbatch_time" \
                 --output="${STORCRAWLDB_OUTPUT_DIR}/output_housekeeper_%a.%J.out" \
                 --cpus-per-task="$sbatch_cpus_per_task" \
                 --job-name="$sbatch_job_name" \
                 --dependency=singleton \
                 --wrap="$0 --action cleanup")
  echo "crawl tidy job launched - jobid ${jobid}"
}

function storcrawl_pwalk {

  my_name="$FUNCNAME (${SLURM_ARRAY_JOB_ID}.${SLURM_JOB_ID}) on $(hostname)"

  storcrawl_log "$my_name"

  # checks
  if [[ -z $SLURM_ARRAY_TASK_ID ]]
  then
    storcrawl_log "$my_name: no SLURM_ARRAY_TASK_ID found, aborting"
    error_exit "You need to run this script as a slurm array job"
  fi

  if [ ! $(which $owner_script) ]
  then
    storcrawl_log "$my_name: owner script $owner_script bad, aborting"
    error_exit "Owner script $owner_script could not be executed"
  fi

  # `which` only returns for executables
  if [ ! $(which $db_cli) ] || [ ! $(which $sbatch_cmd) ] || [ ! $(which $pwalk_cmd) ]
  then
    storcrawl_log "$my_name: bad db_cli: $db_cli, sbatch_cmd: $sbatch_cmd, or pwalk_cmd: $pwalk_cmd, aborting"
    error_exit "Unable to execute $db_cli, $sbatch_cmd, or $pwalk_cmd"
  fi
    
  # this sets pwalk_start_path
  get_folder
  storcrawl_log "$my_name: got folder $pwalk_start_path"

  # determine owner
  my_owner=$($owner_script $pwalk_start_path)
  storcrawl_log "$my_name: got owner $my_owner"

  # determine server/export
  # this sets $pwalk_export
  get_export "$pwalk_start_path"
  # this sets $pwalk_server
  get_server "$pwalk_start_path"

  storcrawl_log "$my_name: ${pwalk_server}:${pwalk_export} - getting source id"

  # get source_id - this sets $pwalk_source_id
  get_source_id "$pwalk_server" "$pwalk_export"

  # create csv (data) filename - needs to have folder name, but also source id to avoid collisions
  folder_path_filename="$(echo $pwalk_start_path | tr '/' '_')"
  csv_filename="${pwalk_source_id}${folder_path_filename}.csv"

  storcrawl_log "$my_name: source id $pwalk_source_id (${pwalk_server}:${pwalk_export})"

  $pwalk_cmd --NoSnap --maxthreads=32 $pwalk_start_path > "${STORCRAWLDB_CSV_DIR}/${csv_filename}"
  storcrawl_log "$my_name done with pwalk"

  # the output of pwalk (15 fields)
  #   inode, parent_inode, directory_depth, filename, fileextension, UID, GID, st_size, st_blocks, st_mode,
  #   st_atime, st_mtime, st_ctime, count, sum
  #
  # [count is the count of inodes contained in the directory, or -1 if not a directory
  #  sum in the sum in bytes of all files in the directory]

  # importing to file table
  storcrawl_log "$my_name importing file table"
  ${csvquote_cmd} "${STORCRAWLDB_CSV_DIR}/$csv_filename" | uconv -s -i | sort -t, -k 4,4 -u | awk -F, -v fs_source_id="${pwalk_source_id}" -v owner="${my_owner}" '{print fs_source_id","owner","$0}' | ${csvquote_cmd} -u | psql $db_conn_str -c "COPY ${STORCRAWLDB_FILE_TABLE}(fs_id,owner,st_ino,parent_inode,directory_depth,filename,fileextension,st_uid,st_gid,st_size,st_blocks,st_mode,st_atime,st_mtime,st_ctime,count,sum) FROM STDIN WITH csv ESCAPE '\'"

  if [ $? -eq 0 ]
  then 
    storcrawl_log "$my_name successfully ran COPY"
  else
    storcrawl_log "$my_name failed during COPY - see ${STORCRAWLDB_OUTPUT_DIR}/output_${SLURM_ARRAY_TASK_ID}_${SLURM_ARRAY_JOB_ID}.${SLURM_JOB_ID}.out"
    error_exit "COPY failed"
  fi

  # update for folder
  folder_crawl_finish

  storcrawl_log "$my_name all done"
} 

function storcrawl_tidy {
  storcrawl_log $FUNCNAME
  # gererate report
  generate_report
  # update ro user
  update_ro_grants
  # update view
  create_storcrawldb_views
  # check for old tables and clean
  clean_tables
  # archive here
  # run after scripts
  run_scripts "${after_script_dir}"
}

function list_tags_from_db {
  list_tags
  exit
}

function remove_tag_from_db {
  remove_tag $specified_tag
  exit
}

function print_log_for_tag {
  print_log
  exit
}

function print_report_for_tag {
  print_report
  exit
}

function print_owner_report_for_tag {
  owner_report
  exit
}

function print_folders_for_tag {
  print_folders
  exit
}

## main tree
if [ "$storcrawl_action" = "start" ]
then
  storcrawl_start
elif [ "$storcrawl_action" = "crawl" ]
then
  storcrawl_pwalk
elif [ "$storcrawl_action" = "cleanup" ]
then
  storcrawl_tidy
elif [ "$storcrawl_action" = "list-tags" ]
then
  list_tags_from_db
elif [ "$storcrawl_action" = "remove-tag" ]
then
  if [ -z "$specified_tag" ]
  then
    error_exit "Tag not specified."
  fi
  remove_tag_from_db $specified_tag
elif [ "$storcrawl_action" = "print-log" ]
then
  if [ -z "$specified_tag" ]
  then
    error_exit "Tag not specified."
  fi
  print_log_for_tag
elif [ "$storcrawl_action" = "print-report" ]
then
  if [ -z "$specified_tag" ]
  then
    error_exit "Tag not specified."
  fi
  print_report_for_tag
elif [ "$storcrawl_action" = "print-owner-report" ]
then
  if [ -z "$specified_tag" ]
  then
    error_exit "Tag not specified."
  fi
  print_owner_report_for_tag
elif [ "$storcrawl_action" = "print-folders" ]
then
  if [ -z "$specified_tag" ]
  then
    error_exit "Tag not specified."
  fi
  print_folders_for_tag
else
  (>&2 echo "Invalid action specified: $storcrawl_action")
  usage
fi

