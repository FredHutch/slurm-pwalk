## generate tag
# my_tag=$(gen_tag [supplied tag])
function gen_tag {
  if [[ $1 =~ ^[0-9]+$ ]]
  then
    echo $1
  else
    date +%Y%m%d%H%M
  fi
}

# usage
function usage {
  printf "\nUsage: %s [options]\n" "$0"
  printf "    %-18.18s  %-18.18s  %-s\n" "Param" "Value" "Description"
  printf "  Required:\n"
  printf "    %-18.18s  %-18.18s  %-s\n" "--config-file" "$storcrawl_config" "storcrawldb.config file"
  printf "    %-18.18s  %-18.18s  %-s\n" "--action" "start" "start, (crawl), cleanup, list-tags, remove-tag"
  printf "  Optional:\n"
  printf "    %-18.18s  %-18.18s  %-s\n" "--tag" "None" "tag target if action requires"
  printf "    %-18.18s  %-18.18s  %-s\n" "--force" "no" "set to 'yes' to remove DB view"
  printf "  The actions are:\n"
  printf "    start - begin a scan - this is where you would normally begin a crawl\n"
  printf "    (crawl) - run pwalk on a folder - this in an internally called action\n"
  printf "    cleanup - run in the batch system at the end to tidy things\n"
  printf "    list-tags - list all tags currently in DB\n"
  printf "    remove-tag - remove all tables related to tag specified with --tag parameter from DB\n"
  printf "    print-log - print logs for -tag parameter from DB\n"
  printf "    print-report - print report for -tag parameter from DB\n"
  printf "    print-folders - print folder list for --tag parameter from DB\n"
  printf "    print-owner-report - print space by owner report for --tag parameter from DB\n"
  printf "\n\n"
  exit 1
}

# set up our environment based on the tag supplied
# export_env $my_tag
function export_env {
  local my_tag="$1"
  export CRAWL_TAG=${CRAWL_TAG:=$my_tag}
  export CRAWL_FILE_TABLE=${CRAWL_FILE_TABLE:="${file_tbl}${CRAWL_TAG}"}
  export CRAWL_FOLDER_TABLE=${CRAWL_FOLDER_TABLE:="${folder_tbl}${CRAWL_TAG}"}
  export CRAWL_DB_LOG_TABLE=${CRAWL_DB_LOG_TABLE:="${log_tbl}${CRAWL_TAG}"}
  export CRAWL_SOURCE_TABLE=${CRAWL_SOURCE_TABLE:="${source_tbl}${CRAWL_TAG}"}
  export CRAWL_REPORT_TABLE=${CRAWL_REPORT_TABLE:="${report_tbl}${CRAWL_TAG}"}
  export CRAWL_CSV_DIR=${CRAWL_CSV_DIR:="${csv_dir}${CRAWL_TAG}"}
  export CRAWL_OUTPUT_DIR=${CRAWL_OUTPUT_DIR:="${output_dir}${CRAWL_TAG}"}
  export CRAWL_VIEW=${CRAWL_VIEW:=$storcrawldb_view}
  export CRAWL_DB_CMD=${CRAWL_DB_CMD:=$db_cmd}
  export CRAWL_PWALK_CMD=${CRAWL_PWALK_CMD:=$pwalk_cmd}
  export CRAWL_SCHEDULER_CMD=${CRAWL_SCHEDULER_CMD:=$scheduler_cmd}
  export CRAWL_CSVQUOTE_CMD=${CRAWL_CVSQUOTE_CMD:=$csvquote_cmd}
}

function storcrawl_log {
  ts=$(date +'%s')
  tz=$(date +'%Z')
  hn=$(hostname)

  log_msg="{"
  log_msg="$log_msg \"timestamp\":\"${ts}\""
  log_msg="$log_msg, \"timezone\":\"${tz}\""
  log_msg="$log_msg, \"hostname\":\"${hn}\""
  log_msg="$log_msg, \"script\":\"${0}\""
  log_msg="$log_msg, \"function\":\"${FUNCNAME}\""
  log_msg="$log_msg, \"pid\":\"${$}\""

  if [ ! -z "${SLURM_JOB_ID}" ]
  then
    log_msg="$log_msg, \"slurm_job_id\":\"${SLURM_JOB_ID}\""
    log_msg="$log_msg, \"slurm_array_job_id\":\"${SLURM_ARRAY_JOB_ID}\""
    log_msg="$log_msg, \"slurm_array_task_id\":\"${SLURM_ARRAY_TASK_ID}\""
    log_msg="$log_msg, \"slurm_job_name\":\"${SLURM_JOB_NAME}\""
    log_msg="$log_msg, \"slurm_job_partition\":\"${SLURM_JOB_PARTITION}\""
    log_msg="$log_msg, \"slurm_nodename\":\"${SLURM_NODENAME}\""
  fi

  log_msg="${log_msg}, \"${1}\":\"${2}\""
    
  log_msg="${log_msg} }"

  ${storcrawl_log_func} "${log_msg}"
}

function run_scripts {
  for script in ${1}/*
  do
    if [ -f $script -a -x $script ]
    then
      echo "running ${script}..."
      $script
      if [ "$?" -eq "0" ]
      then
        echo "$script ran successfully"
      else
        error_exit "$script failed to run"
      fi
    fi
  done
}

# listens on STDIN for: owner/tag, foldername
# owner/tag is optional - any string on a line w/o a comma is foldername
function build_folder_table {
  while read line
  do
    case "$line" in
    *,*)
      folder=${line#*,}
      owner=${line%,*}
      ;;
    *)
      folder="$line"
      ;;
    esac
    my_folder_id=$(add_folder $folder $owner)
    echo "$folder added with id $my_folder_id and owner $owner"
  done
}

# find_mount ${path}
# uses findmnt to find source of path, recurses up tree until mount is found
# as folders below the mount point may not allow non-root to see mount info
function find_mount {
  s=$(findmnt -n -o source --target "${1}")
  if [ $? -eq 0 ]
  then
    echo "${s}"
  else
    find_mount $(dirname "${1}")
  fi
}

# export=$(get_export ${folder_name})
function get_export {
  # returns device name for direct-attach storage
  mySource=find_mount "${1}"
  echo $mySource | grep -q ':'
  if [ $? -eq 0 ]
  then
    echo $mySource | cut -f2 -d:
  else
    echo $mySource
  fi
}

# server=$(get_server ${folder_name})
function get_server {
  myServer=find_mount "${1}"
  echo $myServer | grep -q ':'
  if [ $? -eq 0 ]
  then
    echo $myServer | cut -f1 -d:
  else
    hostname
  fi
}

# add_folder ${folder_name} ${owner}
function add_folder {
  local folder="${1}"
  local owner="${2}"
  local server=$(get_server ${folder})
  local export=$(get_export ${folder})
  local fs_id=$(get_fs_id ${server} ${export})
  db_add_folder ${folder} ${fs_id} ${owner}
}

# wrapper functions (call db for now)
function get_folder_id {
  db_get_folder_id ${@}
}

function get_folder_detail {
  db_get_folder_detail ${@}
}

function get_fs_id {
  db_get_fs_id ${@}
}

function run_crawl_jobs {
  slurm_run_crawl_jobs ${@}
}

function run_cleanup_job {
  slurm_run_cleanup_job ${@}
}

function build_exclusion_list_file {
  db_build_exclusion_list_file ${@}
}

# check output dirs
function check_output_dirs {
  if [ ! -d "$CRAWL_CSV_DIR" ]
  then
    mkdir -p $CRAWL_CSV_DIR ||\
      error_exit "Unable to create CSV dir $CRAWL_CSV_DIR"
  fi
  if [ ! -w "$CRAWL_CSV_DIR" ]
  then
    error_exit "Unable to write to CSV dir $CRAWL_CSV_DIR"
  fi
  if [ ! -d "$CRAWL_OUTPUT_DIR" ]
  then
    mkdir -p $CRAWL_OUTPUT_DIR ||\
      error_exit "Unable to create OUTPUT dir $CRAWL_OUTPUT_DIR"
  fi
  if [ ! -w "$CRAWL_OUTPUT_DIR" ]
  then
    error_exit "Unable to write to OUTPUT dir $CRAWL_OUTPUT_DIR"
  fi
}

function check_array {
  if [[ -z $SLURM_ARRAY_TASK_ID ]]
  then
    error_exit "No array task id found - run as a slurm array job only!"
  fi
}

function check_exec {
  # `which` only returns for executables
  if [ ! $(which $1) ]
  then
    error_exit "Unable to execute $1"
  fi
}

# run_pwalk ${folder_name} ${output}
function run_pwalk {
  local folder_name="${1}"
  local output_file="${2}"
  local csv_out="${CRAWL_CSV_DIR}/${output_file}.csv"
  local excl_file="${CRAWL_CSV_DIR}/${output_file}.exclusion_list"
  build_exclusion_list_file "${folder_name}" "${excl_file}"
  # run pwalk
  $pwalk_cmd $pwalk_opts --exclude "${excl_file}" "${folder_name}" > "${csv_out}"
  storcrawl_log "pwalk exit" "${?}"
}

# print error and exit
function error_exit {
  echo "${PROGNAME} (tag $CRAWL_TAG): $1" 1>&2
  exit 1
}

