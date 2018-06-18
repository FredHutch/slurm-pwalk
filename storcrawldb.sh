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
  clean_tables
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
  # wait for a bit for slurm to settle
  sleep 60
  slack_msg "${slack_text}"
  #run_requeue_job
  run_cleanup_job
  sleep 60
  #run_monitor_job
  log_time=$(date +%Y%m%d%H%M%S)
  while true
  do
    jobs_running=$($queue_cmd --partition=${sbatch_partition} --name ${sbatch_job_name} --Format=name --noheader --array --state=RUNNING | wc -l)
    jobs_pending=$($queue_cmd --partition=${sbatch_partition} --name ${sbatch_job_name} --Format=name --noheader --array --state=PENDING | wc -l)
    jobs_failed=$($acct_cmd --partition=${sbatch_partition} --name ${sbatch_job_name} -s failed -n -S $(date +%Y-%m-%d -d '2 days ago') -E $(date +%Y-%m-%d) -X | wc -l)
    if [ "$jobs_running" -eq "0" ]
    then
      # send email
      ./storcrawldb.sh --action print-folders-not-crawled --tag ${CRAWL_TAG} \
      | mail -s "storcrawl ${CRAWL_TAG} folders not crawled" bmcgough@fredhutch.org
      echo "all jobs finished, exiting"
      exit 0
    fi
    loop_time=$(date +%Y%m%d%H%M%S)
    let elapsed_time="${loop_time}-${log_time}"
    if [ "${elapsed_time}" -gt "${monitor_log_interval}" ]
    then
      slack_msg "${slack_text} ${jobs_running}/${jobs_pending}/${jobs_failed} jobs running/pending/failed"
      storcrawl_log "${jobs_running}/${jobs_pending}/${jobs_dailed} jobs running/pending/failed"
      echo "${jobs_running}/${jobs_pending}/${jobs_failed} jobs running/pending/failed"
      log_time=$loop_time
    fi
    sleep ${monitor_check_interval}
  done

}

# execute pwalk - this is what each job will run
function storcrawl_pwalk {

  echo "storcrawldb running with action crawl"
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
  out="$(echo ${name} | tr '/' '_')"

  run_pwalk "${name}" "${out}"
  if [ "${?}" -eq "0" ]
  then
    echo "crawl of ${name} successful"
  else
    echo "crawl of ${name} failed with exit ${?}"
  fi
  #fs_id=$(add_folder_fs_id "${name}")
  #echo "DEBUG: importing data for ${name} under fs_id ${fs_id}"
  #import_crawl_csv "${out}" "${fs_id}" "${owner}"
  import_crawl_csv "${out}" "${owner}"
  if [ "${?}" -eq "0" ]
  then
    echo "import of ${out} successful"
    folder_crawl_finish "${id}"
  else
    echo "import of ${out} failed with exit ${?}"
  fi

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
    if [ "${jobs_running}" -eq "0" ]
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

# requeue - run after initial job arrays are done to re-try failed folders
function storcrawl_requeue {

  echo "storcrawldb running with action requeue"
  storcrawl_log "crawl requeue start"
  if [ "${CRAWL_REQUEUE_NUM}" -le "${CRAWL_REQUEUE_LIMIT}" ]
  then
    run_crawl_jobs
    export CRAWL_REQUEUE_NUM=$((${CRAWL_REQUEUE_NUM}+1))
    run_requeue_job
  else
    run_cleanup_job
  fi

}

# known as cleanup, this is run by one job at the end of the crawl
function storcrawl_tidy {

  echo "storcrawldb running with action tidy"
  slack_msg "crawl ${CRAWL_TAG}: housekeeping running"
  storcrawl_log "crawl housekeeping start"
  generate_report
  create_storcrawldb_views
  update_ro_grants
  #clean_tables
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
function print_folders_crawled_for_tag {
  print_folders_crawled
  exit
}

function print_folders_not_crawled_for_tag {
  print_folders_not_crawled
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
elif [ "$storcrawl_action" = "requeue" ]
then
  storcrawl_requeue
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
elif [ "$storcrawl_action" = "print-folders-crawled" ]
then
  if [ -z "$specified_tag" ]
  then
    error_exit "Tag not specified."
  fi
  print_folders_crawled_for_tag
elif [ "$storcrawl_action" = "print-folders-not-crawled" ]
then
  if [ -z "$specified_tag" ]
  then
    error_exit "Tag not specified."
  fi
  print_folders_not_crawled_for_tag 
else
  (>&2 echo "Invalid action specified: $storcrawl_action")
  usage
fi

