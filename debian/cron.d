#
# Regular cron jobs for the blitzortung-tracker package
#

SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# m h dom mon dow user  command
# retrieve stroke and station data and insert into local db
#* * * * *	root	ps >/dev/null -C vacuumdb || bo-retrieve && touch /tmp/.bo-retrieve.last
# retrieve raw data and add to local archive
#15 * * * *	root	bo-retrieve-raw &>/dev/null && touch /tmp/.bo-retrieve-raw.last
# write webservice log to database
#*/5 * * * *	root	bo-webservice-insertlog 
