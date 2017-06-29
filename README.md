# slurm-pwalk
Use [pwalk](https://github.com/fizwit/filesystem-reporting-tools) running on your slurm cluster to walk big file systems and store metadata in a PostgreSQL database.

## Requirements
* slurm
* pwalk
* PostgreSQL
* [csvquote](https://github.com/dbro/csvquote)
* [Lmod](https://github.com/TACC/Lmod)

## How to use
1. Clone repo
2. Install requirements (and ensure paths in storcrawldb.config are accurate)
3. create dirs: before_scripts.d after_scripts.d remove_scripts.d
4. Create a PostgreSQL database
5. Edit storcrawldb.config to suit your environment
6. run `stocrawldb.sh`

## Recommendations
* run system as a normal user, make the pwalk binary setuid 0 to scan as root as needed
* fast storage for your database

## Details
Each crawl auto-generates a TAG of the current timestamp to the minute. This TAG is appended to table names, and is how you identiy each crawl. The TAG can be specified on the commandline, which is required to delete scans, print logs, reports, etc.

Output is done to two directories, which must be on cluster shared storage. The metadata goes into `csv_<TAG>` and the job output goes into `output_<TAG>`.

Custom local scripts are run from several directories, and have access to the current TAG. Examples of things done in these scripts: sync UID/GID names from local system into DB for use in queries and views (and remove those tables later), create data and output directory symlinks, etc.

A brief log is kept and a report generated at the end of the scan. These are available through `storcrawldb.sh`.

## Note on our storage_chargeback_ownership setup
We have a private repo on github that allows collaboration on specifying ownership of folders. The storcrawl system will include some auxiliary scripts to use this repo to supply ownership information during the copy of pwalk output into the database.

To use this, you will need to set up a Deploy Key with github. I found this to be most helpful:
https://www.justinsilver.com/technology/github-multiple-repository-ssh-deploy-keys/
