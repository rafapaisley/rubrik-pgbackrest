# rubrik-pgbackrest
A custom script to get pgbackrest working with Rubrik Managed Volume

Help:
./pgbackrest.sh -h
Usage: ./pgbackrest.sh -t TYPE -c CONFIG
    -t TYPE   : Backup type (auto | full | incr | archive | wal | mount | unmount)
    -c CONFIG : Full path for the config file
    -s WAL File source (Only applicable when using -t wal)
    -d WAL File destination (Only applicable when using -t wal)
INFO: '-t wal' should ONLY be used by archive_command. Do not run it manually if you are not sure of what you are doing

auto -> takes a full or an incremental backup based on days_between_full variable


How to use it ?

Adjust the script to your needs and update your archive_command as this:

postgres=# show archive_command;
                                           archive_command
-----------------------------------------------------------------------------------------------------
 /opt/app/postgres/scripts/pgbackrest.sh -t wal -c /opt/app/postgres/conf/pg-backup.conf -s %p -d %f
