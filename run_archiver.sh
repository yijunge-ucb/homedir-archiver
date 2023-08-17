#!/usr/bin/bash

# Simple script that runs the archiver for each filestore
# Below are examples for what gets run, note the archiver runs twice.  The first run uploads archives, the second one actually does the deletion.

# TMPDIR=/export/tmp /usr/bin/python3 /root/archiver/homedir-archiver/archiver/scanner.py /export/data100homes-2020-08-04/data100/prod 180 gs://ucb-datahub-archived-homedirs/2022-3-fall/data100 > /root/archiver/logs/2022-3-fall/data100.log 2> /root/archiver/logs/2022-3-fall/data100_error.log
# TMPDIR=/export/tmp /usr/bin/python3 /root/archiver/homedir-archiver/archiver/scanner.py /export/data100homes-2020-08-04/data100/prod 180 gs://ucb-datahub-archived-homedirs/2022-3-fall/data100 --delete > /root/archiver/logs/2022-3-fall/data100_delete.log 2> /root/archiver/logs/2022-3-fall/data100_delete_error.log

# tar.gz files are staged for upload here
export TMPDIR="/export/tmp"

# gcp storage bucket
BUCKET="gs://ucb-datahub-archived-homedirs"
# designates a folder which will contain additional subfolders for each hub.  Each hub subfolder contains the archives.
# i chose the notation below to ensure proper sorting.  Otherwise YYYY-fall would sort before YYYY-spring or YYYY-summer.
TERM="2023-2-summer"
# how long a user has to be inactive to be considered for archival
DAYS=180
# where the logs go
LOGDIR="/root/archiver/logs/${TERM}"

mkdir -p ${LOGDIR}

# for each filesystem in /export
for fs in /export/*filestore ; do
  # make sure it is a dir, not tmp, and not a symlink
  if [ -d "${fs}" ] && [ "${fs}" != "${TMPDIR}" ] && [ ! -L "${fs}" ]; then
    # foreach hub in each filesystem
    for hubdir in ${fs}/*; do
      # make sure it is a dir and not a symlink
      if [ -d "${hubdir}" ] && [ ! -L "${hubdir}" ]; then
        # make sure the prod files are a dir and not a symlink
        if [ -d "${hubdir}/prod" ] && [ ! -L "${hubdir}/prod" ]; then
          hub="$(basename -- $hubdir)"
          echo ${hub}
          COMMAND=(/usr/bin/python3 /root/archiver/homedir-archiver/archiver/scanner.py "${hubdir}/prod" ${DAYS} "${BUCKET}/${TERM}/${hub}")
          "${COMMAND[@]}" > "${LOGDIR}/${hub}.log" 2> "${LOGDIR}/${hub}_error.log"
          COMMAND=(/usr/bin/python3 /root/archiver/homedir-archiver/archiver/scanner.py "${hubdir}/prod" ${DAYS} "${BUCKET}/${TERM}/${hub}" --delete)
          "${COMMAND[@]}" > "${LOGDIR}/${hub}_delete.log" 2> "${LOGDIR}/${hub}_delete_error.log"
        fi
      fi
    done
  fi
done
