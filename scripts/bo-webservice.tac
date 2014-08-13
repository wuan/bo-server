#!/usr/bin/env python

from __future__ import division, print_function
from twisted.internet.defer import gatherResults

from twisted.python import log

try:
    from twisted.internet import epollreactor as reactor
except ImportError:
    from twisted.internet import kqreactor as reactor

try:
    reactor.install()
except ReactorAlreadyInstalledError:
    pass

from twisted.internet import reactor as the_reactor
from twisted.internet import defer
from twisted.internet.error import ReactorAlreadyInstalledError
from txjsonrpc.web.jsonrpc import with_request

from zope.interface import Interface, implements
from twisted.cred import portal, checkers, credentials, error as credential_error
from twisted.cred.checkers import InMemoryUsernamePasswordDatabaseDontUse
from twisted.web import server, http
from twisted.web.guard import HTTPAuthSessionWrapper, DigestCredentialFactory
from twisted.application import service, internet
from twisted.python.log import ILogObserver, FileLogObserver
from twisted.python.logfile import DailyLogFile

from txjsonrpc.auth import wrapResource
from txjsonrpc.web import jsonrpc

import math
import datetime
import time
import pyproj
import pytz
import statsd

statsd_client = statsd.StatsClient('localhost', 8125, prefix='org.blitzortung.service')

import blitzortung.config
import blitzortung.cache
import blitzortung.geom
import blitzortung.db
import blitzortung.db.query
import blitzortung_server

from blitzortung_server.db import compile_strikes_result

import sys

if sys.version > '3':
    long = int

WGS84 = pyproj.Proj(init='epsg:4326')
UTM_EU = pyproj.Proj(init='epsg:32633')  # UTM 33 N / WGS84
UTM_USA = pyproj.Proj(init='epsg:32614')  # UTM 14 N / WGS84
UTM_OC = pyproj.Proj(init='epsg:32755')  # UTM 55 S / WGS84


class PasswordDictChecker(object):
    implements(checkers.ICredentialsChecker)
    credentialInterfaces = (credentials.IUsernamePassword,)

    def __init__(self, passwords):
        self.passwords = passwords

    def requestAvatarId(self, credentials):
        username = credentials.username
        if username in self.passwords:
            if credentials.password == self.passwords[username]:
                return defer.succeed(username)
        return defer.fail(credential_error.UnathorizedLogin("invalid username/password"))


class IUserAvatar(Interface):
    """ should have attribute username """


class UserAvatar(object):
    implements(IUserAvatar)

    def __init__(self, username):
        self.username = username


class TestRealm(object):
    implements(portal.IRealm)

    def __init__(self, users):
        self.users = users

    def requestAvatar(self, avatarId, mind, *interfaces):
        if IUserAvatar in interfaces:
            logout = lambda: None
            return (IUserAvatar,
                    UserAvatar(avatarId),
                    logout)
        else:
            raise KeyError('none of the requested interfaces is supported')


class GridDataFactory(object):
    def __init__(self, min_lon, max_lon, min_lat, max_lat, coord_sys):
        self.min_lon = min_lon
        self.max_lon = max_lon
        self.min_lat = min_lat
        self.max_lat = max_lat
        self.coord_sys = coord_sys

        self.grid_data = {}

    @staticmethod
    def fix_max(minimum, maximum, delta):
        return minimum + math.floor((maximum - minimum) / delta) * delta

    def get_for(self, base_length):
        if base_length not in self.grid_data:
            ref_lon = (self.min_lon + self.max_lon) / 2.0
            ref_lat = (self.min_lat + self.max_lat) / 2.0

            utm_x, utm_y = pyproj.transform(WGS84, self.coord_sys, ref_lon, ref_lat)
            lon_d, lat_d = pyproj.transform(self.coord_sys, WGS84, utm_x + base_length, utm_y + base_length)

            delta_lon = lon_d - ref_lon
            delta_lat = lat_d - ref_lat

            max_lon = self.fix_max(self.min_lon, self.max_lon, delta_lon)
            max_lat = self.fix_max(self.min_lat, self.max_lat, delta_lat)

            self.grid_data[base_length] = blitzortung.geom.Grid(self.min_lon, max_lon, self.min_lat, max_lat,
                                                                delta_lon, delta_lat,
                                                                blitzortung.geom.Geometry.DefaultSrid)

        return self.grid_data[base_length]


grid = {1: GridDataFactory(-15, 40, 32, 70, UTM_EU),
        2: GridDataFactory(110, 180, -50, 0, UTM_OC),
        3: GridDataFactory(-140, -50, 10, 60, UTM_USA)}


class WrappedDeferred(defer.Deferred):

    def callback(self, result):
        print("callback(", result, ")")
        defer.Deferred.callback(self, result)

    def chainDeferred(self, d):
        print("chainDeferred", d)
        return defer.Deferred.chainDeferred(self, d)

    def cancel(self):
        print("cancel()")
        defer.Deferred.cancel(self)

    def _startRunCallbacks(self, result):
        print("_startRunCallbacks()")
        defer.Deferred._startRunCallbacks(self, result)

    def addBoth(self, callback, *args, **kw):
        print("addBoth()")
        return defer.Deferred.addBoth(self, callback, *args, **kw)

    def _continuation(self):
        print("_continuation()")
        return defer.Deferred._continuation(self)

    def errback(self, fail=None):
        print("errback()")
        defer.Deferred.errback(self, fail)

    def pause(self):
        print("pause()")
        defer.Deferred.pause(self)

    def _runCallbacks(self):
        result = self.getResult()
        for callback in self.callbacks:
            pass
            #print("    ", callback)
        defer.Deferred._runCallbacks(self)
        print("  _runCallbacks()", self.getResult())
        #if result:
        #    self.result = result

    def addCallback(self, callback, *args, **kw):
        print("addCallback()", callback, args, kw, self.getResult())
        return defer.Deferred.addCallback(self, callback, *args, **kw)

    def addCallbacks(self, callback, errback=None, callbackArgs=None, callbackKeywords=None, errbackArgs=None,
                     errbackKeywords=None):
        print("addCallbacks()", callback, self.getResult())
        return defer.Deferred.addCallbacks(self, callback, errback, callbackArgs, callbackKeywords, errbackArgs,
                                           errbackKeywords)

    def addErrback(self, errback, *args, **kw):
        print("addErrback()", errback, args, kw, self.getResult())
        return defer.Deferred.addErrback(self, errback, *args, **kw)

    def unpause(self):
        print("unpause()")
        defer.Deferred.unpause(self)

    def getResult(self):
        if hasattr(self, 'result'):
            return self.result
        else:
            return None

class Blitzortung(jsonrpc.JSONRPC):
    """
    An example object to be published.
    """

    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.strike_query_builder = blitzortung.db.query_builder.Strike()
        self.check_count = 0
        self.strikes_grid_cache = blitzortung.cache.ObjectCache(ttl_seconds=20)
        self.test = None

    addSlash = True

    @staticmethod
    def __force_min(number, min_number):
        return max(min_number, number)

    @staticmethod
    def __force_max(number, max_number):
        return min(max_number, number)

    def __force_range(self, number, min_number, max_number):
        return self.__force_min(self.__force_max(number, max_number), min_number)

    def jsonrpc_check(self):
        self.check_count += 1
        return {'count': self.check_count}

    @with_request
    def jsonrpc_get_strokes(self, request, minute_length, id_or_offset=0):
        return self.jsonrpc_get_strikes(request, minute_length, id_or_offset)

    def create_strikes_query(self, id_or_offset, minute_length, minute_offset, reference_time):
        query, end_time = self.create_strike_query(id_or_offset, minute_length, minute_offset)
        strikes_query = self.connection_pool.runQuery(str(query), query.get_parameters())
        strikes_query.addCallback(blitzortung_server.strike_result_build, end_time=end_time,
                                  statsd_client=statsd_client, reference_time=reference_time)
        return strikes_query, end_time

    def create_histogram_query(self, minute_length, minute_offset):
        reference_time = time.time()
        query = self.strike_query_builder.histogram_query(blitzortung.db.table.Strike.TABLE_NAME, minute_length,
                                                          minute_offset, 5)
        histogram_query = self.connection_pool.runQuery(str(query), query.get_parameters())
        histogram_query.addCallback(blitzortung_server.histogram_result_build, minutes=minute_length, bin_size=5,
                                    reference_time=reference_time)
        return histogram_query

    @with_request
    def jsonrpc_get_strikes(self, request, minute_length, id_or_offset=0):
        minute_length = self.__force_range(minute_length, 0, 24 * 60)
        minute_offset = self.__force_range(id_or_offset, -24 * 60 + minute_length,
                                           0) if id_or_offset < 0 else 0

        reference_time = time.time()

        strikes_query, end_time = self.create_strikes_query(id_or_offset, minute_length, minute_offset, reference_time)

        minute_offset = -id_or_offset if id_or_offset < 0 else 0
        histogram_query = self.create_histogram_query(minute_length, minute_offset)

        query = gatherResults([strikes_query, histogram_query], consumeErrors=True)
        query.addCallback(compile_strikes_result, end_time=end_time)
        query.addErrback(log.err)

        client = self.get_request_client(request)
        user_agent = request.getHeader("User-Agent")
        print('"get_strikes(%d, %d)" %s "%s"' % ( minute_length, id_or_offset, client, user_agent))

        full_time = time.time()
        statsd_client.incr(blitzortung.db.table.Strike.TABLE_NAME)
        statsd_client.timing(blitzortung.db.table.Strike.TABLE_NAME, max(1, int((full_time - reference_time) * 1000)))

        return query

    def create_time_interval(self, minute_length, minute_offset):
        end_time = datetime.datetime.utcnow()
        end_time = end_time.replace(tzinfo=pytz.UTC)
        end_time = end_time.replace(microsecond=0)
        end_time += datetime.timedelta(minutes=minute_offset)
        start_time = end_time - datetime.timedelta(minutes=minute_length)
        time_interval = blitzortung.db.query.TimeInterval(start_time, end_time)
        return time_interval

    def create_strike_query(self, id_or_offset, minute_length, minute_offset):
        time_interval = self.create_time_interval(minute_length, minute_offset)

        if id_or_offset > 0:
            id_interval = blitzortung.db.query.IdInterval(id_or_offset)
        else:
            id_interval = None

        order = blitzortung.db.query.Order('id')

        return self.strike_query_builder.select_query(blitzortung.db.table.Strike.TABLE_NAME,
                                                      blitzortung.geom.Geometry.DefaultSrid, time_interval,
                                                      id_interval, order), time_interval.get_end()

    def jsonrpc_get_strikes_around(self, longitude, latitude, minute_length, min_id=None):
        pass

    def create_strikes_grid_query(self, grid_parameters, minute_length, minute_offset, reference_time):
        time_interval = self.create_time_interval(minute_length, minute_offset)

        query = self.strike_query_builder.grid_query(blitzortung.db.table.Strike.TABLE_NAME, grid_parameters,
                                                     time_interval)
        grid_query = self.connection_pool.runQuery(str(query), query.get_parameters())
        grid_query.addCallback(self.build_grid_result, end_time=time_interval.get_end(),
                               statsd_client=statsd_client, reference_time=reference_time,
                               grid_parameters=grid_parameters)
        grid_query.addErrback(log.err)
        return grid_query, time_interval.get_end()

    def get_strikes_grid(self, minute_length, grid_baselength, minute_offset, region):

        grid_parameters = grid[region].get_for(grid_baselength)

        reference_time = time.time()

        grid_query, end_time = self.create_strikes_grid_query(grid_parameters, minute_length, minute_offset,
                                                              reference_time)

        histogram_query = self.create_histogram_query(minute_length, minute_offset)

        query = gatherResults([grid_query, histogram_query], consumeErrors=True)
        query.addCallback(self.build_grid_response, grid_parameters=grid_parameters, end_time=end_time)
        query.addErrback(log.err)

        return query

    def build_grid_result(self, results, end_time, statsd_client, reference_time, grid_parameters):
        query_duration = time.time() - reference_time
        print("strikes_grid_query() %.02fs #%d %s" % (query_duration, len(results), grid_parameters))
        statsd_client.timing('strikes_grid.query', max(1, int(query_duration * 1000)))

        reference_time = time.time()
        grid_data = blitzortung.data.GridData(grid_parameters)

        for result in results:
            grid_data.set(result['rx'], result['ry'],
                          blitzortung.geom.GridElement(result['count'], result['timestamp']))

        reduced_array = grid_data.to_reduced_array(end_time)

        statsd_client.timing('strikes_grid.reduce', max(1, int((time.time() - reference_time) * 1000)))
        return reduced_array

    def build_grid_response(self, results, end_time, grid_parameters):
        grid_data = results[0]
        histogram_data = results[1]

        statsd_client.gauge('strikes_grid.size', len(grid_data))
        statsd_client.incr('strikes_grid')

        reference_time = time.time()
        response = {'r': grid_data, 'xd': round(grid_parameters.get_x_div(), 6),
                    'yd': round(grid_parameters.get_y_div(), 6),
                    'x0': round(grid_parameters.get_x_min(), 4), 'y1': round(grid_parameters.get_y_max(), 4),
                    'xc': grid_parameters.get_x_bin_count(),
                    'yc': grid_parameters.get_y_bin_count(), 't': end_time.strftime("%Y%m%dT%H:%M:%S"),
                    'h': histogram_data}
        statsd_client.timing('strikes_grid.pack_response', max(1, int((time.time() - reference_time) * 1000)))
        return response


    @with_request
    def jsonrpc_get_strikes_raster(self, request, minute_length, grid_base_length=10000, minute_offset=0, region=1):
        return self.jsonrpc_get_strikes_grid(request, minute_length, grid_base_length, minute_offset, region)

    @with_request
    def jsonrpc_get_strokes_raster(self, request, minute_length, grid_base_length=10000, minute_offset=0, region=1):
        return self.jsonrpc_get_strikes_grid(request, minute_length, grid_base_length, minute_offset, region)

    @with_request
    def jsonrpc_get_strikes_grid(self, request, minute_length, grid_base_length=10000, minute_offset=0, region=1):
        grid_base_length = self.__force_min(grid_base_length, 5000)
        minute_length = self.__force_range(minute_length, 0, 24 * 60)
        minute_offset = self.__force_range(minute_offset, -24 * 60 + minute_length, 0)

        response = self.strikes_grid_cache.get(self.get_strikes_grid, minute_length=minute_length,
                                               grid_baselength=grid_base_length,
                                               minute_offset=minute_offset, region=region)

        client = self.get_request_client(request)
        user_agent = request.getHeader("User-Agent")
        print('"get_strikes_grid(%d, %d, %d, %d)" "%.1f%%" %s "%s"' % (
            minute_length, grid_base_length, minute_offset, region, self.strikes_grid_cache.get_ratio() * 100, client,
            user_agent))

        return response

    @with_request
    def jsonrpc_get_stations(self, request):
        stations_db = blitzortung.db.station()

        reference_time = time.time()
        stations = stations_db.select()
        query_time = time.time()
        statsd_client.timing('stations.query', max(1, int((query_time - reference_time) * 1000)))

        station_data = tuple(
            (
                station.get_number(),
                station.get_name(),
                station.get_country(),
                station.get_x(),
                station.get_y(),
                station.get_timestamp().strftime("%Y%m%dT%H:%M:%S.%f")[:-3] if station.get_timestamp() else ''
            )
            for station in stations
        )

        response = {'stations': station_data}

        full_time = time.time()

        client = self.get_request_client(request)
        user_agent = request.getHeader("User-Agent")
        print('"get_stations()" "#%d %.2fs" %s "%s"' % (len(stations), query_time - reference_time, client, user_agent))
        statsd_client.incr('stations')
        statsd_client.timing('stations', max(1, int((full_time - reference_time) * 1000)))

        return response

    def get_request_client(self, request):
        forward = request.getHeader("X-Forwarded-For")
        if forward:
            return forward.split(', ')[0]
        return request.getClientIP()


users = {'test': 'test'}

# Set up the application and the JSON-RPC resource.
application = service.Application("Blitzortung.org JSON-RPC Server")
#log_directory = "/var/log/blitzortung"
#log_directory = "./"
#logfile = DailyLogFile("webservice.log", log_directory)
#application.setComponent(ILogObserver, FileLogObserver(logfile).emit)
connection_pool = blitzortung_server.create_connection_pool()
root = Blitzortung(connection_pool)

credentialFactory = DigestCredentialFactory("md5", "blitzortung.org")
# Define the credential checker the application will be using and wrap the JSON-RPC resource.
checker = InMemoryUsernamePasswordDatabaseDontUse()
checker.addUser('test', 'test')
realm_name = "Blitzortung.org JSON-RPC App"
wrappedRoot = wrapResource(root, [checker], realmName=realm_name)


class PublicHTMLRealm(object):
    implements(portal.IRealm)

    def requestAvatar(self, avatarId, mind, *interfaces):
        if IResource in interfaces:
            return (IResource, File("/home/%s/public_html" % (avatarId,)), lambda: None)
        raise NotImplementedError()


portal = portal.Portal(PublicHTMLRealm(), [checker])

resource = HTTPAuthSessionWrapper(portal, [credentialFactory])

# With the wrapped root, we can set up the server as usual.
# site = server.Site(resource=wrappedRoot)
config = blitzortung.config.config()
site = server.Site(root)
site.displayTracebacks = False
jsonrpc_server = internet.TCPServer(config.get_webservice_port(), site)
jsonrpc_server.setServiceParent(application)
