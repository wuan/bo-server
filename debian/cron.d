#
# Regular cron jobs for the blitzortung-tracker package
#

SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# m h dom mon dow user  command
# import stroke and station data into local db and calculate clusters from current stroke data
#* * * * *      root    ps >/dev/null -C vacuumdb || (bo-import && touch /tmp/.bo-import.last) && (bo-cluster -m import && touch /tmp/bo-cluster.last)
# write webservice log to database
#* * * * *      root    bo-webservice-insertlog >/dev/null
# import raw data and add to local archive
#15 * * * * 	root	bo-import-raw &>/dev/null && touch /tmp/.bo-import-raw.last
