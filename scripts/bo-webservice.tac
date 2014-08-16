#!/usr/bin/env python

from __future__ import division, print_function
import psycopg2
from twisted.internet.defer import gatherResults
from twisted.internet.error import ReactorAlreadyInstalledError

from twisted.python import log
from twisted.web.resource import IResource
from twisted.web.static import File
from txpostgres import reconnection
from txpostgres.txpostgres import Connection, ConnectionPool

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

import os
import time
import pyproj
import statsd

statsd_client = statsd.StatsClient('localhost', 8125, prefix='org.blitzortung.service')

import blitzortung.builder
import blitzortung.config
import blitzortung.cache
import blitzortung.data
import blitzortung.geom
import blitzortung.db
import blitzortung.db.mapper
import blitzortung.db.query
import blitzortung.db.query_builder

import blitzortung_server.service

import sys

if sys.version > '3':
    long = int

UTM_EU = pyproj.Proj(init='epsg:32633')  # UTM 33 N / WGS84
UTM_USA = pyproj.Proj(init='epsg:32614')  # UTM 14 N / WGS84
UTM_OC = pyproj.Proj(init='epsg:32755')  # UTM 55 S / WGS84


def connection_factory(*args, **kwargs):
    kwargs['connection_factory'] = psycopg2.extras.DictConnection
    return psycopg2.connect(*args, **kwargs)


class LoggingDetector(reconnection.DeadConnectionDetector):
    def startReconnecting(self, f):
        print('[*] database connection is down (error: %r)' % f.value)
        return reconnection.DeadConnectionDetector.startReconnecting(self, f)

    def reconnect(self):
        print('[*] reconnecting...')
        return reconnection.DeadConnectionDetector.reconnect(self)

    def connectionRecovered(self):
        print('[*] connection recovered')
        return reconnection.DeadConnectionDetector.connectionRecovered(self)


class DictConnection(Connection):
    connectionFactory = staticmethod(connection_factory)

    def __init__(self, reactor=None, cooperator=None, detector=None):
        if not detector:
            detector = LoggingDetector()
        super(DictConnection, self).__init__(reactor, cooperator, detector)


class DictConnectionPool(ConnectionPool):
    connectionFactory = DictConnection

    def __init__(self, _ignored, *connargs, **connkw):
        super(DictConnectionPool, self).__init__(_ignored, *connargs, **connkw)


def create_connection_pool():
    config = blitzortung.config.config()
    db_connection_string = config.get_db_connection_string()

    created_connection_pool = DictConnectionPool(None, db_connection_string)

    d = created_connection_pool.start()
    d.addErrback(log.err)
    return created_connection_pool


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


grid = {1: blitzortung.geom.GridFactory(-15, 40, 32, 70, UTM_EU),
        2: blitzortung.geom.GridFactory(110, 180, -50, 0, UTM_OC),
        3: blitzortung.geom.GridFactory(-140, -50, 10, 60, UTM_USA)}


class Blitzortung(jsonrpc.JSONRPC):
    """
    An example object to be published.
    """

    def __init__(self, db_connection_pool):
        self.connection_pool = db_connection_pool
        self.strike_query = blitzortung_server.service.strike_query()
        self.strike_grid_query = blitzortung_server.service.strike_grid_query()
        self.histogram_query = blitzortung_server.service.histogram_query()
        self.check_count = 0
        self.strikes_grid_cache = blitzortung.cache.ObjectCache(ttl_seconds=20)
        self.histogram_cache = blitzortung.cache.ObjectCache(ttl_seconds=60)
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


    @with_request
    def jsonrpc_get_strikes(self, request, minute_length, id_or_offset=0):
        minute_length = self.__force_range(minute_length, 0, 24 * 60)
        minute_offset = self.__force_range(id_or_offset, -24 * 60 + minute_length,
                                           0) if id_or_offset < 0 else 0

        strikes_result, state = self.strike_query.create(id_or_offset, minute_length, minute_offset,
                                                         self.connection_pool, statsd_client)

        minute_offset = -id_or_offset if id_or_offset < 0 else 0
        histogram_result = self.get_histogram(minute_length, minute_offset)

        combined_result = self.strike_query.combine_result(strikes_result, histogram_result, state)
        print(combined_result)

        client = self.get_request_client(request)
        user_agent = request.getHeader("User-Agent")
        print('"get_strikes(%d, %d)" %s "%s"' % (minute_length, id_or_offset, client, user_agent))
        print(combined_result)
        return combined_result

    def jsonrpc_get_strikes_around(self, longitude, latitude, minute_length, min_id=None):
        pass

    def get_strikes_grid(self, minute_length, grid_baselength, minute_offset, region):
        grid_parameters = grid[region].get_for(grid_baselength)

        grid_result, state = self.strike_grid_query.create(grid_parameters, minute_length, minute_offset,
                                                           self.connection_pool, statsd_client)

        histogram_result = self.get_histogram(minute_length, minute_offset, region)

        combined_result = self.strike_grid_query.combine_result(grid_result, histogram_result, state)

        return combined_result

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

    def get_histogram(self, minute_length, minute_offset, region=None):
        return self.histogram_cache.get(self.histogram_query.create,
                                        connection=self.connection_pool,
                                        minute_length=minute_length,
                                        minute_offset=minute_offset,
                                        region=region)

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
log_directory = "/var/log/blitzortung"
if os.path.exists(log_directory):
    logfile = DailyLogFile("webservice.log", log_directory)
    application.setComponent(ILogObserver, FileLogObserver(logfile).emit)
connection_pool = create_connection_pool()
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
            return IResource, File("/home/%s/public_html" % (avatarId,)), lambda: None
        raise NotImplementedError()


service_portal = portal.Portal(PublicHTMLRealm(), [checker])

resource = HTTPAuthSessionWrapper(service_portal, [credentialFactory])

# With the wrapped root, we can set up the server as usual.
# site = server.Site(resource=wrappedRoot)
config = blitzortung.config.config()
site = server.Site(root)
site.displayTracebacks = False
jsonrpc_server = internet.TCPServer(config.get_webservice_port(), site)
jsonrpc_server.setServiceParent(application)
