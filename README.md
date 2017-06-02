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
2. Install requirements
3. Create a PostgreSQL database
4. Edit storcrawldb.config to suit your environment
5. run `stocrawldb.sh`

## Recommendations
* run system as a normal user, make the pwalk binary setuid 0 to scan as root
* fast storage for your database

## Details
Each crawl auto-generates a TAG of the current timestamp to the minute. This TAG is appended to table names, and is how you identiy each crawl. The TAG can be specified on the commandline, which is required to delete scans, print logs, reports, etc.

Output is done to a single directory, which must be on shared storage and should be reasonably fast. The metadata goes into `csv_<TAG>` and the job output goes into `output_<TAG>`.

Custom local scripts are run from several directories, and have access to the current TAG. Examples of things done in these scripts: sync UID/GID names from local system into DB (and remove those tables later), create data and output directory symlinks, etc.

A brief log is kept and a report generated at the end of the scan. These are available through `storcrawldb.sh`.
