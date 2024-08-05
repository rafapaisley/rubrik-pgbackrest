#!/bin/bash

### Global functions ###
source_configFile () {
  source "${configFile}"
  export PGPASSWORD
}

set_mv_rw () {
  FileCheck="${LOCAL_MOUNT}/FileCheck.$(date +%Y%m%d%H%M%S)"
  RWStatus=0
  Retries=0
  touch ${FileCheck} > /dev/null 2>&1 # try to write to filesystem
  if [ $? -ne 0 ]; then
    while [ ${RWStatus} -ne 200 ] && [ ${RWStatus} -ne 422 ] && [ ${Retries} -le 5 ]; do
      RWStatus=$(curl ${CURL_MAX_TIMEOUT} -s -o /dev/null -w "%{http_code}" -k -X POST "${RUBRIK_URL}/api/internal/managed_volume/${MV_ID}/begin_snapshot" -H  "accept: application/json" -H  "Authorization: Bearer $TOKEN")
      if [ ${RWStatus} -ne 200 ] && [ ${RWStatus} -ne 422 ] && [ ${Retries} -le 5 ]; then
        ((Retries++))
        sleep 5
      fi
    done
    if [ ${RWStatus} -eq 422 ]; then
      log_to_file "INFO" "MV already in ReadWrite mode"
    elif [ ${RWStatus} -ne 200 ]; then
      log_to_file "ERROR" "Failed to set MV as ReadWrite. Error code: ${RWStatus}"
      error "Failed to set MV as ReadWrite (${RUBRIK_URL}). Error code: ${RWStatus}"
    fi
  else
    rm -rf ${FileCheck}
  fi
}

mount_mv () {
  if [ $(mount -l | grep -iwc ${LOCAL_MOUNT}) -eq 0 ]; then
    sudo mount -o ${MOUNT_OPTIONS} ${MV_EXPORT_IP}:${MV_EXPORT_MOUNTPOINT} ${LOCAL_MOUNT}
    MountStatus=$?
    if [ $MountStatus -ne 0 ]; then
      log_to_file "ERROR" "Failed while mounting MV"
      error "Failed while mounting MV (${RUBRIK_URL})"
    fi
  fi
}

unmount_mv () {
  sudo umount ${LOCAL_MOUNT}
  uMountStatus=$?
  if [ $uMountStatus -ne 0 ]; then
    log_to_file "ERROR" "Failed while unmounting MV"
    error "Failed while unmounting MV (${RUBRIK_URL})"
  fi
}

set_mv_ro () {
  if [ "${backupType}" == "full" ] || [ "${backupType}" == "incr" ]; then
    # Verify if an archive/binlog backup is running before setting MV to ReadOnly
    backupType="archive"
    getLock
    isLocked=$?
    if [ ${isLocked} -ne 0 ]; then
      log_to_file "INFO" "An ${backupType} backup is running. Leaving MV in ReadWrite mode"
      exit 0
    fi
  elif [ "${backupType}" == "archive" ]; then
    # Verify if a full/incr backup is running before setting MV to ReadOnly
    backupType="full/incr"
    getLock
    isLocked=$?
    if [ ${isLocked} -ne 0 ]; then
      log_to_file "INFO" "A ${backupType} backup is running. Leaving MV in ReadWrite mode"
      exit 0
    fi
  fi
  ROStatus=0
  Retries=0
  StatusFile="/var/tmp/${FUNCNAME[0]}.status"
  if [ ! -f ${StatusFile} ]; then
    echo "0" > ${StatusFile}
  fi
  while [ ${ROStatus} -ne 200 ] && [ ${ROStatus} -ne 422 ] && [ ${Retries} -le 5 ]; do
    ROStatus=$(curl ${CURL_MAX_TIMEOUT} -s -o /dev/null -w "%{http_code}" -k -X POST "${RUBRIK_URL}/api/internal/managed_volume/${MV_ID}/end_snapshot" -H  "accept: apication/json" -H  "Authorization: Bearer $TOKEN")
    if [ ${ROStatus} -ne 200 ]  && [ ${ROStatus} -ne 422 ] && [ ${Retries} -le 5 ]; then
      ((Retries++))
      sleep 5
    fi
  done
  if [ ${ROStatus} -ne 200 ] && [ ${ROStatus} -ne 422 ]; then
    ROCount=$((0 + $(cat ${StatusFile})))
    if [ ${ROCount} -ge ${MAX_RO_RETRIES} ]; then
      echo "0" > ${StatusFile}
      log_to_file "ERROR" "Failed to set MV to ReadOnly."
      error "Failed to set MV to ReadOnly (${RUBRIK_URL})."
    else
      ((ROCount++))
      echo ${ROCount} > ${StatusFile}
    fi
  else
    echo "0" > ${StatusFile}
    log_to_file "INFO" "MV successfully set to ReadOnly"
  fi
}

usage() {
  echo "Usage: $0 -t TYPE -c CONFIG"
  echo "    -t TYPE   : Backup type (auto | full | incr | archive | wal | mount | unmount)"
  echo "    -c CONFIG : Full path for the config file"
  echo "    -s WAL File source (Only applicable when using -t wal)"
  echo "    -d WAL File destination (Only applicable when using -t wal)"
  echo "INFO: '-t wal' should ONLY be used by archive_command. Do not run it manually if you are not sure of what you are doing"
}

error () {
  MAIL_MESSAGE=$1
  MAIL_HEADER_DATE=$(date +%FT%R:%S)
  MAIL_HEADER="${HOSTNAME} - ${LOG_SOURCE}: Failed at ${MAIL_HEADER_DATE}"
  echo "${MAIL_MESSAGE}" | mailx -s "${MAIL_HEADER}" "${MAIL_RECIPIENTS}"
  exit 1
}

log_to_file () {
  LOG_LEVEL=$1
  LOG_TEXT=$2
  echo $(date +%FT%R:%S) " ${HOSTNAME} ${LOG_SOURCE}: [${LOG_LEVEL}] ${LOG_TEXT}" >> "${backup_log_file}"
}

isPrimary () {
  # Check if database if primary/single (not in recovery)
  if [ $(psql --quiet --tuples-only -c 'select pg_is_in_recovery ();') == "t" ]; then
    log_to_file "INFO" "Database is in recovery mode and can not be backed up!"
    exit 0
  fi
}

sanity_check () {
  # Check if hostname and port provided
  if [ -z  ${STANZA_NAME} ] ; then
    log_to_file "ERROR" "Cannot find stanza name in the configuration file at ${configFile}"
    error "Cannot find stanza name in the configuration file at ${configFile}"
  fi

  if [ "$(id --user --name)" != "$pg_backup_owner" ]; then
    log_to_file "ERROR" "Script can only be run as the \"$pg_backup_owner\" user"
    error "Script can only be run as the \"$pg_backup_owner\" user"
  fi

  # Check config file parameter
  if [ ! -r ${configfile} ]; then
    error "Cannot find configuration file at ${configfile}"
  fi

  #Check for temp archive files to be renamed
  for file in $(find ${LOCAL_MOUNT}/archive -type f -name *.pgbackrest.tmp)
  do
    new_filename=$(echo $file | cut -d. -f 1-2)
    mv $file $new_filename
  done
}

switch_logfile () {
  log_to_file "INFO" "Switching logfile - pg_switch_wal() "
  psql --quiet --tuples-only -c 'select pg_switch_wal();' > /dev/null
  sleep 60
}


take_backup() {
  log_to_file "INFO" "Starting ${backupType} backup"
  if [ "${backupType}" == "full" ] || [ "${backupType}" == "incr" ]; then
    pgbackrest --stanza=$STANZA_NAME backup --type=$backupType --compress >> $DUMP_LOG
    pgbackrest_status="$(tail -1 ${DUMP_LOG})"
    if [[ ! $pgbackrest_status == *"completed successfully"* ]]; then
      log_to_file "ERROR" "pgbackrest failed! Check log file at ${DUMP_LOG}"
      error "ERROR" "pgbackrest failed! Check log file at ${DUMP_LOG}"
    fi
    log_to_file "INFO" "Database backup complete"
  elif [ "${backupType}" == "archive" ]; then
    ARCHIVELOGS=$(ls -dtr ${archive_dir}/* 2>/dev/null)
    for log in $ARCHIVELOGS ; do
      pgbackrest --stanza=$STANZA_NAME archive-push ${log} >/dev/null 2>&1
      if [ "${?}" -eq "0" ]; then
        log_to_file "INFO" "File ${log} successfully pushed to ${LOCAL_MOUNT}"
        rm ${log}
      else
        log_to_file "ERROR" "Failed to push ${log} to ${LOCAL_MOUNT}"
        error "Failed to push ${log} to ${LOCAL_MOUNT}"
      fi
    done
    switch_logfile
    log_to_file "INFO" "Archivelog backup complete"
  fi
}

wal_switch() {
  FileCheck="${LOCAL_MOUNT}/FileCheck.$(date +%Y%m%d%H%M%S)"
  touch ${FileCheck} > /dev/null 2>&1 # try to write to filesystem
  if [ $? -eq 1 ]; then
    ExecStart=$(date "+%Y-%m-%d %H:%M:%S.%3N")
    echo "${ExecStart} WAL   INFO: wal-copy command begin: [${walFile}]"
    cp ${walFile} ${archive_dir}/${arcFile}
    if [ "${?}" -eq "0" ]; then
      ExecEnd=$(date "+%Y-%m-%d %H:%M:%S.%3N")
      milisecondsStart=$(date --date "$ExecStart" +%s%N)
      milisecondsEnd=$(date --date "$ExecEnd" +%s%N)
      delta=$(((milisecondsEnd - milisecondsStart)/1000000))
      echo "${ExecEnd} WAL   INFO: copied WAL file '${arcFile}' to ${archive_dir}"
      echo "${ExecEnd} WAL   INFO: wal-copy command end: completed successfully [${delta}ms]"
    else
      exit 1
    fi
  else
    rm -f ${FileCheck}
    pgbackrest --stanza=${STANZA_NAME} archive-push ${walFile}
  fi
}

getLock () {
  if [ "${backupType}" == "full" ] || [ "${backupType}" == "incr" ] || [ "${backupType}" == "full/incr" ]; then
    lockType="bkp"
  else
    lockType="arc"
  fi
  exec {lock_fd}>/var/tmp/$(basename $0).${lockType} || exit 1
  flock -n "$lock_fd"
}

waitLock () {
  getLock
  isLocked=$?
  Retries=0
  while [ ${isLocked} -ne 0 ] && [ ${Retries} -lt 10 ]; do
    ((Retries++))
    log_to_file "INFO" "Failed to acquire lock... Retry number ${Retries}"
    sleep 300
    getLock
    isLocked=$?
  done
  if [ ${isLocked} -eq 1 ] || [ ${Retries} -ge 10 ]; then
    error "Timeout acquiring lock... No more retries"
  else
    log_to_file "INFO" "Lock acquired for PID $$"
    mount_mv && set_mv_rw
  fi
}

### Script starts here ###
while getopts ":hc:t:s:d:" options; do
  case "${options}" in
    h)
      usage; exit ;;
    t)
      backupType=${OPTARG}
      if ! [[ "${backupType}" =~ ^(auto|full|incr|archive|wal|mount|unmount)$ ]]; then
        echo -e "\nError: Invalid backup type = ${OPTARG}\n"
        usage; exit 1
      fi
      ;;
    c)
      configFile=${OPTARG}
      if [ ! -f "${configFile}" ]; then
        echo -e "\nError: Config file ${OPTARG} not found!\n"
        usage; exit 1
      fi
      ;;
    s)
      walFile=${OPTARG} ;;
    d)
      arcFile=${OPTARG} ;;
    :)
      echo -e "\nError: -${OPTARG} requires an argument.\n"
      usage; exit 1 ;;
    *)
      usage; exit 1 ;;
  esac
done

if [ -z "${backupType}" ] || [ -z "${configFile}" ]; then
  echo -e "\nError: -t and -c are mandatory\n"
  usage; exit 1
fi


# Source config file
source_configFile

# Take Backup
if [ "${backupType}" == "auto" ]; then
  #Verify if a full backup exists
  if [ $(pgbackrest info --type=full | grep status | grep 'no valid backups' | wc -l) -eq 1 ]; then
    backupType="full"
  else
    #Get latest full backup timestamp
    LatestFullBackup="$(date --date "$(pgbackrest info --type=full | grep timestamp | tail -1 | awk '{print $3,$4}')" +%s)"
    #Get current timestamp
    CurrentTimeStamp="$(date +%s)"
    if  [ $(( (CurrentTimeStamp - LatestFullBackup) / 86400 )) -ge ${days_between_full} ]; then
      backupType="full"
    else
      backupType="incr"
    fi
  fi
  isPrimary && waitLock && sanity_check && take_backup && set_mv_ro
elif [ "${backupType}" == "incr" ]; then
  isPrimary && waitLock && sanity_check && take_backup && set_mv_ro
elif [ "${backupType}" == "full" ]; then
  isPrimary && waitLock && sanity_check && take_backup && set_mv_ro
elif [ "${backupType}" == "archive" ]; then
  isPrimary && waitLock && sanity_check && take_backup && set_mv_ro
elif [ "${backupType}" == "wal" ]; then
  if [ -z "${arcFile}" ] || [ -z "${walFile}" ]; then
    echo -e "\nError: -s and -d are mandatory for backup type ${backupType}\n"
    usage; exit 1
  else
    wal_switch
  fi
elif [ "${backupType}" == "mount" ]; then
  echo "Mounting the volume..."
  mount_mv && set_mv_rw
elif [ "${backupType}" == "unmount" ]; then
  echo "Unmounting the volume..."
  unmount_mv
  exit 0
fi
