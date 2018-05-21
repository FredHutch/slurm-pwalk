#! /bin/bash

PROGNAME=$(basename $0)
DIRNAME=$(dirname $0)

## source config
source $DIRNAME/storcrawldb.config

## source functions
source $DIRNAME/${bash_functions_file}
source $DIRNAME/${db_functions_file}
source $DIRNAME/${scheduler_functions_file}

# gather parameters
# see usage in functions file for parameter definitions
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

# set up tag, then export env vars
my_tag=$(gen_tag $specified_tag)
export_env "$my_tag"

# functions here - main flow @end
#  storcrawldb.sh is called by itself with the --action parameter
#  to determine behavior

# start - create tables, schedule jobs
function storcrawl_start {

  echo "storcrawldb running with action start"
  slack_text="Crawl ${CRAWL_TAG}:"
  init_db
  check_tag
  check_last_crawl
  init_crawl
  storcrawl_log "crawl start"
  folder_count=$(build_folder_table)
  slack_text="${slack_text} ${folder_count} folders"
  set_job_array_size
  run_scripts "${before_script_dir}"
  check_output_dirs
  run_crawl_jobs
  slack_msg "${slack_text}"
  run_cleanup_job
  #run_monitor_job
  log_time=$(date +%Y%m%d%H%M%S)
  while true
  do
    jobs_running=$($queue_cmd --partition=${sbatch_partition} --name ${sbatch_job_name} -o %A -h | wc -l)
    if [ "$jobs_running" -eq "0" ]
    then
      exit 0
    fi
    loop_time=$(date +%Y%m%d%H%M%S)
    let elapsed_time="${loop_time}-${log_time}"
    if [ "${elapsed_time}" -gt "${monitor_log_interval}" ]
    then
      slack_msg "${jobs_running} jobs running"
      log_time=$loop_time
    fi
    sleep ${monitor_check_interval}
  done

}

# execute pwalk - this is what each job will run
function storcrawl_pwalk {

  echo "storcrawldb running with action crawl"
  storcrawl_log "crawl pwalk"
  check_array
  check_exec $CRAWL_DB_CMD
  check_exec $CRAWL_SCHEDULER_CMD
  check_exec $CRAWL_PWALK_CMD
  check_exec $CRAWL_CSVQUOTE_CMD
  export TMPDIR="${SCRATCH}"

  id=$(get_folder_id)
  if [ -z "$id" ]
  then
    echo "no folders found"
    exit 0
  fi
  name=$(get_folder_detail ${id} "folder")
  owner=$(get_folder_detail ${id} "owner")
  fs_id=$(get_folder_detail ${id} "fs_id")
  out="${fs_id}$(echo ${name} | tr '/' '_')"

  echo "DEBUG: storcrawldb running pwalk on folder ${name} to output ${out}"
  run_pwalk "${name}" "${out}"
  import_crawl_csv "${out}" "${fs_id}" "${owner}"
  folder_crawl_finish "${id}"

} 

# monitor jobs during crawl
function storcrawl_monitor {
  echo "storcrawldb running with action monitor"
  storcrawl_log "crawl monitor start"

  log_time=$(date +%Y%m%d%H%M%S)
  sbatch_job_name="${sbatch_job_name_prefix}${CRAWL_TAG}"

  storcrawl_log "monitoring ${sbatch_job_name} jobs"

  while true
  do
    jobs_running=$($queue_cmd --partition=${sbatch_partition} --name ${sbatch_job_name} -o %A -h | wc -l)
    if [ "$jobs_running" -eq "0" ]
    then
      exit 0
    fi
    loop_time=$(date +%Y%m%d%H%M%S)
    elapsed_time=$((${loop_time}-${log_time}))
    if [ "${elapsed_time}" -gt "${monitor_log_interval}" ]
    then
      slack_msg "${jobs_running} jobs running"
      log_time=$loop_time
    fi
    sleep ${monitor_check_interval}
  done
}

# known as cleanup, this is run by one job at the end of the crawl
function storcrawl_tidy {

  echo "storcrawldb running with action tidy"
  storcrawl_log "crawl housekeeping start"
  generate_report
  create_storcrawldb_views
  update_ro_grants
  clean_tables
  # archive here
  run_scripts "${after_script_dir}"
  slack_report

}

# display all knowns tags
function list_tags_from_db {
  list_tags
  exit
}

# delete all tables associated with a tag
function remove_tag_from_db {
  remove_tag $specified_tag
  echo "note that directories associated with this tag are not removed"
  exit
}

# dump the log for a given tag
function print_log_for_tag {
  print_log
  exit
}

# dump the report for a given tag
function print_report_for_tag {
  print_report
  exit
}

# dump the folder/owner list of a given tag
function print_folders_for_tag {
  print_folders
  exit
}

## main flow
if [ "$storcrawl_action" = "start" ]
then
  storcrawl_start
elif [ "$storcrawl_action" = "crawl" ]
then
  storcrawl_pwalk
elif [ "$storcrawl_action" = "monitor" ]
then
  storcrawl_monitor
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
    #echo "No tag specified, trying to find most recent tag..."
    specified_tag=$(get_most_recent_tag)
    if [ -z "$specified_tag" ]
    then
      error_exit "Tag not specified, unable to determine recent tag"
    fi
  fi
  owner_report $specified_tag
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

