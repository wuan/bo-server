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


class RasterDataFactory(object):
    def __init__(self, min_lon, max_lon, min_lat, max_lat, coord_sys):
        self.min_lon = min_lon
        self.max_lon = max_lon
        self.min_lat = min_lat
        self.max_lat = max_lat
        self.coord_sys = coord_sys

        self.raster_data = {}

    @staticmethod
    def fix_max(minimum, maximum, delta):
        return minimum + math.floor((maximum - minimum) / delta) * delta

    def get_for(self, base_length):
        if base_length not in self.raster_data:
            ref_lon = (self.min_lon + self.max_lon) / 2.0
            ref_lat = (self.min_lat + self.max_lat) / 2.0

            utm_x, utm_y = pyproj.transform(WGS84, self.coord_sys, ref_lon, ref_lat)
            lon_d, lat_d = pyproj.transform(self.coord_sys, WGS84, utm_x + base_length, utm_y + base_length)

            delta_lon = lon_d - ref_lon
            delta_lat = lat_d - ref_lat

            max_lon = self.fix_max(self.min_lon, self.max_lon, delta_lon)
            max_lat = self.fix_max(self.min_lat, self.max_lat, delta_lat)

            self.raster_data[base_length] = blitzortung.geom.Raster(self.min_lon, max_lon, self.min_lat, max_lat,
                                                                    delta_lon, delta_lat,
                                                                    blitzortung.geom.Geometry.DefaultSrid)

        return self.raster_data[base_length]


raster = {1: RasterDataFactory(-15, 40, 32, 70, UTM_EU),
          2: RasterDataFactory(110, 180, -50, 0, UTM_OC),
          3: RasterDataFactory(-140, -50, 10, 60, UTM_USA)}


class Blitzortung(jsonrpc.JSONRPC):
    """
    An example object to be published.
    """

    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.query_builder = blitzortung_server.QueryBuilder()
        self.check_count = 0
        self.strikes_raster_cache = blitzortung.cache.ObjectCache(ttl_seconds=20)

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

    def create_strike_query(self, id_or_offset, minute_length, minute_offset):
        end_time = datetime.datetime.utcnow()
        end_time = end_time.replace(tzinfo=pytz.UTC)
        end_time = end_time.replace(microsecond=0)
        end_time += datetime.timedelta(minutes=minute_offset)

        start_time = end_time - datetime.timedelta(minutes=minute_length)
        time_interval = blitzortung.db.query.TimeInterval(start_time, end_time)

        if id_or_offset > 0:
            id_interval = blitzortung.db.query.IdInterval(id_or_offset)
        else:
            id_interval = None

        area = None

        order = blitzortung.db.query.Order('id')
        query = self.query_builder.select_strokes_query(time_interval, id_interval, area, order)

        return query, end_time

    @with_request
    def jsonrpc_get_strikes(self, request, minute_length, id_or_offset=0):
        minute_length = self.__force_range(minute_length, 0, 24 * 60)
        minute_offset = self.__force_range(id_or_offset, -24 * 60 + minute_length,
                                           0) if id_or_offset < 0 else 0

        query, end_time = self.create_strike_query(id_or_offset, minute_length, minute_offset)
        reference_time = time.time()

        strikes_query = self.connection_pool.runQuery(str(query), query.get_parameters())
        strikes_query.addCallback(blitzortung_server.strike_result_build, end_time=end_time,
                                   statsd_client=statsd_client, reference_time=reference_time)

        histogram_query = self.connection_pool.runQuery("select id from strikes order by id desc limit 1")

        query = gatherResults([strikes_query, histogram_query], consumeErrors=True)
        query.addCallback(compile_strikes_result, end_time=end_time)
        query.addErrback(log.err)

        client = self.get_request_client(request)
        user_agent = request.getHeader("User-Agent")
        print('"get_strikes(%d, %d)" %s "%s"' % ( minute_length, id_or_offset, client, user_agent))

        full_time = time.time()
        statsd_client.incr('strikes')
        statsd_client.timing('strikes', max(1, int((full_time - reference_time) * 1000)))

        return query

    def jsonrpc_get_strikes_around(self, longitude, latitude, minute_length, min_id=None):
        pass

    def create_raster_query(self, minute_length, minute_offset, raster_data):
        end_time = datetime.datetime.utcnow()
        end_time = end_time.replace(tzinfo=pytz.UTC, microsecond=0)
        end_time += datetime.timedelta(minutes=minute_offset)
        start_time = end_time - datetime.timedelta(minutes=minute_length)
        time_interval = blitzortung.db.query.TimeInterval(start_time, end_time)
        query = blitzortung.db.query.RasterQuery(raster_data)
        query.add_parameters([time_interval])
        return query, time_interval

    def get_strikes_raster(self, minute_length, raster_baselength, minute_offset, region):

        raster_data = raster[region].get_for(raster_baselength)
        query, time_interval = self.create_raster_query(minute_length, minute_offset, raster_data)

        raster_query = self.connection_pool.runQuery(str(query), query.get_parameters())
        raster_query.addCallback(blitzortung_server.strike_result_build, end_time=time_interval.get_end(),
                                   statsd_client=statsd_client, reference_time=reference_time)

        histogram_query = self.connection_pool.runQuery("select id from strikes order by id desc limit 1")

        query = gatherResults([raster_query, histogram_query], consumeErrors=True)
        query.addCallback(compile_strikes_result, end_time=time_interval.get_end())
        query.addErrback(log.err)

        reference_time = time.time()
        statsd_client.timing('strikes_raster.query', max(1, int((time.time() - reference_time) * 1000)))

        reference_time = time.time()
        reduced_strike_array = raster_strikes.to_reduced_array(end_time)
        statsd_client.timing('strikes_raster.reduce', max(1, int((time.time() - reference_time) * 1000)))

        reference_time = time.time()
        histogram = strike_db.select_histogram(minute_length, minute_offset, 5, envelope=raster_data)
        statsd_client.timing('strikes_raster.histogram_query', max(1, int((time.time() - reference_time) * 1000)))

        reference_time = time.time()
        response = {'r': reduced_strike_array, 'xd': round(raster_data.get_x_div(), 6),
                    'yd': round(raster_data.get_y_div(), 6),
                    'x0': round(raster_data.get_x_min(), 4), 'y1': round(raster_data.get_y_max(), 4),
                    'xc': raster_data.get_x_bin_count(),
                    'yc': raster_data.get_y_bin_count(), 't': end_time.strftime("%Y%m%dT%H:%M:%S"),
                    'h': histogram}
        statsd_client.timing('strikes_raster.pack_response', max(1, int((time.time() - reference_time) * 1000)))

        return response

    @with_request
    def jsonrpc_get_strokes_raster(self, request, minute_length, raster_base_length=10000, minute_offset=0, region=1):
        return self.jsonrpc_get_strikes_raster(request, minute_length, raster_base_length, minute_offset, region)

    @with_request
    def jsonrpc_get_strikes_raster(self, request, minute_length, raster_base_length=10000, minute_offset=0, region=1):
        raster_base_length = self.__force_min(raster_base_length, 5000)
        minute_length = self.__force_range(minute_length, 0, 24 * 60)
        minute_offset = self.__force_range(minute_offset, -24 * 60 + minute_length, 0)

        reference_time = time.time()

        response = self.strikes_raster_cache.get(self.get_strikes_raster, minute_length=minute_length,
                                                 raster_baselength=raster_base_length,
                                                 minute_offset=minute_offset, region=region)

        full_time = time.time() - reference_time
        data_size = len(response['r'])

        statsd_client.incr('strikes_raster')
        statsd_client.timing('strikes_raster', max(1, int(full_time * 1000)))
        statsd_client.gauge('strikes_raster.size', data_size)

        client = self.get_request_client(request)
        user_agent = request.getHeader("User-Agent")
        print('"get_strikes_raster(%d, %d, %d, %d)" "#%d %.2fs %.1f%%" %s "%s"' % (
            minute_length, raster_base_length, minute_offset, region, data_size, full_time,
            self.strikes_raster_cache.get_ratio() * 100, client, user_agent))

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
