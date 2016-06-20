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
import datetime
import pytz
import pyproj
import statsd
import json
import collections

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
import blitzortung.service

import sys

if sys.version > '3':
    long = int

UTM_EU = pyproj.Proj(init='epsg:32633')  # UTM 33 N / WGS84
UTM_NORTH_AMERICA = pyproj.Proj(init='epsg:32614')  # UTM 14 N / WGS84
UTM_CENTRAL_AMERICA = pyproj.Proj(init='epsg:32614')  # UTM 14 N / WGS84
UTM_SOUTH_AMERICA = pyproj.Proj(init='epsg:32720')  # UTM 20 S / WGS84
UTM_OCEANIA = pyproj.Proj(init='epsg:32755')  # UTM 55 S / WGS84
UTM_ASIA = pyproj.Proj(init='epsg:32650')  # UTM 50 N / WGS84
UTM_AFRICA = pyproj.Proj(init='epsg:32633')  # UTM 33 N / WGS84


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


grid = {
    1: blitzortung.geom.GridFactory(-20, 57, 27, 70, UTM_EU),
    2: blitzortung.geom.GridFactory(110, 180, -50, 0, UTM_OCEANIA),
    3: blitzortung.geom.GridFactory(-140, -50, 10, 60, UTM_NORTH_AMERICA),
    4: blitzortung.geom.GridFactory(85, 150, -10, 60, UTM_ASIA),
    5: blitzortung.geom.GridFactory(-100, -30, -50, 20, UTM_SOUTH_AMERICA),
    6: blitzortung.geom.GridFactory(-20, 50, -40, 40, UTM_AFRICA),
    7: blitzortung.geom.GridFactory(-115, -50, 0, 30, UTM_CENTRAL_AMERICA)
}


class Blitzortung(jsonrpc.JSONRPC):
    """
    An example object to be published.
    """

    def __init__(self, db_connection_pool, log_directory):
        self.connection_pool = db_connection_pool
        self.log_directory = log_directory
        self.strike_query = blitzortung.service.strike_query()
        self.strike_grid_query = blitzortung.service.strike_grid_query()
        self.histogram_query = blitzortung.service.histogram_query()
        self.check_count = 0
        self.strikes_grid_cache = blitzortung.cache.ObjectCache(ttl_seconds=20)
        self.histogram_cache = blitzortung.cache.ObjectCache(ttl_seconds=60)
        self.test = None
        self.current_period = self.__current_period()
        self.current_data = collections.defaultdict(list)

    addSlash = True

    def __current_period(self):
        return datetime.datetime.utcnow().replace(second=0, microsecond=0, tzinfo=pytz.UTC)

    def __check_period(self):
        if self.current_period != self.__current_period():
            self.current_data['timestamp'] = int(self.current_period.strftime("%s"))
            with open(os.path.join(log_directory, self.current_period.strftime("%Y%m%d-%H%M.json")),
                      'w') as output_file:
                output_file.write(json.dumps(self.current_data))
            self.__restart_period()

    def __restart_period(self):
        self.current_period = self.__current_period()
        self.current_data = collections.defaultdict(list)

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
    def jsonrpc_get_strikes(self, request, minute_length, id_or_offset=0):
        minute_length = self.__force_range(minute_length, 0, 24 * 60)

        client = self.get_request_client(request)
        user_agent = request.getHeader("User-Agent")
        print('"get_strikes(%d, %d)" %s "%s DISABLED"' % (minute_length, id_or_offset, client, user_agent))
        return None

        minute_offset = self.__force_range(id_or_offset, -24 * 60 + minute_length, 0) if id_or_offset < 0 else 0
        strikes_result, state = self.strike_query.create(id_or_offset, minute_length, minute_offset,
                                                         self.connection_pool, statsd_client)

        histogram_result = self.get_histogram(minute_length, minute_offset)

        combined_result = self.strike_query.combine_result(strikes_result, histogram_result, state)

        print('"get_strikes(%d, %d)" %s "%s"' % (minute_length, id_or_offset, client, user_agent))
        return combined_result

    def jsonrpc_get_strikes_around(self, longitude, latitude, minute_length, min_id=None):
        pass

    def get_strikes_grid(self, minute_length, grid_baselength, minute_offset, region, count_threshold):
        grid_parameters = grid[region].get_for(grid_baselength)

        grid_result, state = self.strike_grid_query.create(grid_parameters, minute_length, minute_offset,
                                                           count_threshold, self.connection_pool, statsd_client)

        histogram_result = self.get_histogram(minute_length, minute_offset, envelope=grid_parameters) \
            if minute_length > 10 else self.deferred_with([])

        combined_result = self.strike_grid_query.combine_result(grid_result, histogram_result, state)

        return combined_result

    def deferred_with(self, result):
        deferred = defer.Deferred()
        deferred.result = result
        deferred.called = True
        return deferred

    @with_request
    def jsonrpc_get_strikes_raster(self, request, minute_length, grid_base_length=10000, minute_offset=0, region=1):
        return self.jsonrpc_get_strikes_grid(request, minute_length, grid_base_length, minute_offset, region)

    @with_request
    def jsonrpc_get_strokes_raster(self, request, minute_length, grid_base_length=10000, minute_offset=0, region=1):
        return self.jsonrpc_get_strikes_grid(request, minute_length, grid_base_length, minute_offset, region)

    @with_request
    def jsonrpc_get_strikes_grid(self, request, minute_length, grid_base_length=10000, minute_offset=0, region=1,
                                 count_threshold=0):
        grid_base_length = self.__force_min(grid_base_length, 5000)
        minute_length = self.__force_range(minute_length, 0, 24 * 60)
        minute_length = 60 if minute_length == 0 else minute_length
        minute_offset = self.__force_range(minute_offset, -24 * 60 + minute_length, 0)
        region = self.__force_min(region, 1)
        count_threshold = self.__force_min(count_threshold, 0)

        response = self.strikes_grid_cache.get(self.get_strikes_grid, minute_length=minute_length,
                                               grid_baselength=grid_base_length,
                                               minute_offset=minute_offset, region=region,
                                               count_threshold=count_threshold)

        client = self.get_request_client(request)
        user_agent = request.getHeader("User-Agent")

        self.fix_bad_accept_header(request, user_agent)

        print('"get_strikes_grid(%d, %d, %d, %d, >=%d)" "%.1f%%" %s "%s"' % (
            minute_length, grid_base_length, minute_offset, region, count_threshold,
            self.strikes_grid_cache.get_ratio() * 100, client, user_agent))

        self.__check_period()
        self.current_data['get_strikes_grid'].append(
            (minute_length, grid_base_length, minute_offset, region, count_threshold, client, user_agent))

        statsd_client.incr('get_strikes_grid')

        return response

    def fix_bad_accept_header(self, request, user_agent):
        if user_agent is not None:
            user_agent_parts = user_agent.split(' ')[0].rsplit('-', 1)
            version_string = user_agent_parts[1] if len(user_agent_parts) > 1 else None
            if user_agent_parts[0] == 'bo-android':
                version = int(version_string)
            else:
                version = None
            if version and version <= 177:
                request.requestHeaders.removeHeader("Accept-Encoding")

    def get_histogram(self, minute_length, minute_offset, region=None, envelope=None):
        return self.histogram_cache.get(self.histogram_query.create,
                                        connection=self.connection_pool,
                                        minute_length=minute_length,
                                        minute_offset=minute_offset,
                                        region=region,
                                        envelope=envelope)

    @with_request
    def jsonrpc_get_clusters(self, minute_length, minute_offset, interval_length, interval_offset):
        pass

    @with_request
    def jsonrpc_get_stations(self, request):
        stations_db = blitzortung.db.station()

        reference_time = time.time()
        stations = stations_db.select()
        query_time = time.time()
        statsd_client.timing('stations.query', max(1, int((query_time - reference_time) * 1000)))

        station_data = tuple(
            (
                station.number,
                station.name,
                station.country,
                station.x,
                station.y,
                station.timestamp.strftime("%Y%m%dT%H:%M:%S.%f")[:-3] if station.timestamp else ''
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
root = Blitzortung(connection_pool, log_directory)

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
