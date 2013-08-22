#
# Regular cron jobs for the blitzortung-tracker package
#

SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# m h dom mon dow user  command
# import stroke and station data and insert into local db
#* * * * *	root	ps >/dev/null -C vacuumdb || bo-import && touch /tmp/.bo-import.last
# import raw data and add to local archive
#15 * * * *	root	bo-import-raw &>/dev/null && touch /tmp/.bo-import-raw.last
# write webservice log to database
#*/5 * * * *	root	bo-webservice-insertlog 
