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
  printf "    (monitor) - run int he batch system to monitor job arrays\n"
  printf "    (cleanup) - run in the batch system at the end to tidy things\n"
  printf "    list-tags - list all tags currently in DB\n"
  printf "    remove-tag - remove all tables related to tag specified with --tag parameter from DB\n"
  printf "    print-log - print logs for -tag parameter from DB\n"
  printf "    print-report - print report for -tag parameter from DB\n"
  printf "    print-folders-crawled - print folders crawled for --tag parameter from DB\n"
  printf "    print-folders-not-crawled - print folders not crawled for --tag parameter from DB\n"
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
  export CRAWL_REQUEUE_NUM=${CRAWL_REQUEUE_NUM:=1}
  export CRAWL_REQUEUE_LIMIT=${CRAWL_REQUEUE_LIMIT:=$requeue_limit}
}

# following builds a JSON log message and logs it
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

  if [ -n "${SLURM_JOB_ID}" ]
  then
    log_msg="$log_msg, \"slurm_job_id\":\"${SLURM_JOB_ID}\""
    log_msg="$log_msg, \"slurm_array_job_id\":\"${SLURM_ARRAY_JOB_ID}\""
    log_msg="$log_msg, \"slurm_array_task_id\":\"${SLURM_ARRAY_TASK_ID}\""
    log_msg="$log_msg, \"slurm_job_name\":\"${SLURM_JOB_NAME}\""
    log_msg="$log_msg, \"slurm_job_partition\":\"${SLURM_JOB_PARTITION}\""
    log_msg="$log_msg, \"slurm_nodename\":\"${SLURM_NODENAME}\""
  fi

  log_msg="${log_msg}, \"text\":\"${1}\""
    
  log_msg="${log_msg} }"

  ${storcrawl_log_func} "${log_msg}"
}

# the following builds a JSON payload and then POSTs to slack webhook using curl
# if the slack_url is undefined, it just exits
function slack_msg {
  if [ -n "$slack_webhook_url" ]
  then
    payload="{\"text\":\"${1}\"}"
    #echo "DEBUG: slack payload is ${payload} and slack_webhook_url is ${slack_webhook_url}"
    curl -X POST --data-urlencode "payload=$payload" "$slack_webhook_url"
  fi
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
# echoes the total number of folders processed
function build_folder_table {
  my_count=0
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
    add_folder "${folder}" "${owner}"
    let my_count=$my_count+1
  done
  echo "${my_count}"
}

# find_mount ${path}
# use `df` which is more reliable
# return format: <hostname>[:server]:<export/device>
# you get back "hostname:server:export or device" for network file systems and
# "hostname:device" for local mounts - use accordingly
function find_mount {
  hn=$(hostname)
  fs=$(df --output=source "${1}" | tail -1)

  echo "${hn}:${fs}"
}

# export=$(get_export ${folder_name})
function get_export {
  # returns 2 or 3 fields
  exp=$(find_mount "${1}" | awk -F: '{ print $NF }')
  if [ "${exp}" == "" ]
  then
    storcrawl_log "found no export for ${1}"
    echo ""
  else
    echo "${exp}"
  fi
}

# server=$(get_server ${folder_name})
function get_server {
  # returns 2 or 3 fields
  srv=$(find_mount "${1}" | awk -F: '{ if (NF>2) {print $2} else {print $1} }')
  if [ "${srv}" == "" ]
  then
    storcrawl_log "found no server for ${1}"
  else
    echo "${srv}"
  fi
}

# add_folder ${folder_name} ${owner}
function add_folder {
  local folder="${1}"
  local owner="${2}"
  db_add_folder "${folder}" "${owner}"
}

# add_folder_fs_id $folder_name
function add_folder_fs_id {

  local folder="${1}"
  local server=$(get_server "${folder}")
  local export=$(get_export "${folder}")
  local fs_id=$(get_fs_id "${server}" "${export}")
  local folder_id=$(get_folder_id "${folder}")
  db_add_folder_fs_id "${folder_id}" "${fs_id}"
  echo "${fs_id}"
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

function run_monitor_job {
  slurm_run_monitor_job ${@}
}

function run_requeue_job {
  slurm_run_requeue_job ${@}
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
  storcrawl_log "$SLURM_JOB_ID running: $pwalk_cmd $pwalk_opts --exclude ${excl_file} ${folder_name}"
  $pwalk_cmd $pwalk_opts --exclude "${excl_file}" "${folder_name}" > "${csv_out}"
  storcrawl_log "$SLURM_JOB_ID pwalk exit: ${?}"
}

# print error and exit
function error_exit {
  echo "${PROGNAME} (tag $CRAWL_TAG): $1" 1>&2
  exit 1
}

