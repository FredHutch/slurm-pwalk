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
  init_db
  check_tag
  check_last_crawl
  init_crawl
  storcrawl_log "action" "start"
  build_folder_table
  set_job_array_size
  run_scripts "${before_script_dir}"
  check_output_dirs
  run_crawl_jobs
  run_cleanup_job

}

# execute pwalk - this is what each job will run
function storcrawl_pwalk {

  echo "storcrawldb running with action crawl"
  storcrawl_log "action" "crawl"
  check_array
  check_exec $CRAWL_DB_CMD
  check_exec $CRAWL_SCHEDULER_CMD
  check_exec $CRAWL_PWALK_CMD
  check_exec $CRAWL_CSVQUOTE_CMD

  id=$(get_folder_id)
  name=$(get_folder_detail ${id} "folder")
  owner=$(get_folder_detail ${id} "owner")
  fs_id=$(get_folder_detail ${id} "fs_id")
  out="${fs_id}_$(echo ${name} | tr '/' '_')"

  echo "storcrawldb running pwalk on folder ${name} to output ${out}"
  run_pwalk ${name} ${out}
  import_crawl_csv ${out} ${fs_id} ${owner}
  folder_crawl_finish ${id}

} 

# known as cleanup, this is run by one job at the end of the crawl
function storcrawl_tidy {

  echo "storcrawldb running with action tidy"
  storcrawl_log "action" "tidy"
  generate_report
  update_ro_grants
  create_storcrawldb_views
  clean_tables
  # archive here
  run_scripts "${after_script_dir}"

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

