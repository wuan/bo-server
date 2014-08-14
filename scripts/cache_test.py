#!/usr/bin/env python

import time

from twisted.internet import reactor
from twisted.internet.defer import gatherResults
from twisted.python import log

from twisted.enterprise import adbapi
from blitzortung.cache import ObjectCache


cache = ObjectCache(ttl_seconds=2)

dbpool = adbapi.ConnectionPool('psycopg2', user="blitzortung", password="blitzortung", database="blitzortung",
                               host="localhost", cp_reconnect=True)


def get_result():
    print("get_result()")

    timestamp_query = do_get_timestamp_query(dbpool)
    count_query = do_count_query(dbpool)
    last_element_query = do_last_element_query(dbpool)
    query = gatherResults([timestamp_query, count_query, last_element_query], consumeErrors=True)
    query.addCallback(provide_result)
    query.addErrback(log.err)
    return query


def do_count_query(dbpool):
    print("do_count_query()")
    return dbpool.runQuery("select count(*) from strikes")


def do_get_timestamp_query(dbpool):
    print("do_get_timestamp_query()")
    return dbpool.runQuery("select clock_timestamp()")


def do_last_element_query(dbpool):
    print("do_last_element_query()")
    return dbpool.runQuery(
        "select timestamp, st_x(geog::geometry), st_y(geog::geometry) from strikes order by id desc limit 1")


def provide_result(results):
    timestamp_result = results[0][0]
    count_result = results[1][0]
    last_element = results[2][0]
    print("provide_result()", str(timestamp_result[0]), count_result, last_element[1])
    return "count %d, %s at %f,%f" % (count_result[0], str(last_element[0]), last_element[1], last_element[2])


def print_result(result, number):
    print("result#%d: %s" % (number, result))
    return result


result1 = cache.get(get_result)
result1.addCallback(print_result, 1)
print("result #1:", result1)

time.sleep(0.5)

result2 = cache.get(get_result)
result2.addCallback(print_result, 2)
print("result #2:", result2)

time.sleep(2)

result3 = cache.get(get_result)
result3.addCallback(print_result, 3)
print("result #3:", result3)

print("done")

reactor.callLater(10, reactor.stop)
reactor.run()
