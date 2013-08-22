#!/usr/bin/env python

from __future__ import division

from twisted.internet import epollreactor, defer

epollreactor.install()

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
import json
import statsd

json.encoder.FLOAT_REPR = lambda f: ("%.4f" % f)

statsd_client = statsd.StatsClient('localhost', 8125, prefix='org.blitzortung.service')

import blitzortung

WGS84 = pyproj.Proj(init='epsg:4326')
UTM_EU = pyproj.Proj(init='epsg:32633')   # UTM 33 N / WGS84
UTM_USA = pyproj.Proj(init='epsg:32614')  # UTM 14 N / WGS84
UTM_OC = pyproj.Proj(init='epsg:32755')   # UTM 55 S / WGS84


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

    def fix_max(self, minimum, maximum, delta):
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

    def __init__(self):
        self.check_count = 0
        self.strokes_raster_cache = blitzortung.cache.ObjectCache(ttl_seconds=20)

    addSlash = True

    def __force_min(self, number, min_number):
        return max(min_number, number)

    def __force_max(self, number, max_number):
        return min(max_number, number)

    def __force_range(self, number, min_number, max_number):
        return self.__force_min(self.__force_max(number, max_number), min_number)

    def jsonrpc_check(self):
        self.check_count += 1
        return {'count': self.check_count}

    def jsonrpc_get_strokes(self, minute_length, id_or_offset=0):
        minute_length = self.__force_range(minute_length, 0, 24 * 60)
        minute_offset = self.__force_range(id_or_offset, -24 * 60 + minute_length,
                                           0) if id_or_offset < 0 else 0

        stroke_db = blitzortung.db.stroke()

        end_time = datetime.datetime.utcnow()
        end_time = end_time.replace(tzinfo=pytz.UTC)
        end_time = end_time.replace(microsecond=0)
        end_time += datetime.timedelta(minutes=minute_offset)

        start_time = end_time - datetime.timedelta(minutes=minute_length)
        time_interval = blitzortung.db.TimeInterval(start_time, end_time)

        if id_or_offset > 0:
            id_interval = blitzortung.db.IdInterval(id_or_offset)
        else:
            id_interval = None

        area = None
        order = blitzortung.db.Order('id')

        reference_time = time.time()
        strokes = stroke_db.select(time_interval, id_interval, area, order)
        query_time = time.time()
        db_query_time = (query_time - reference_time)
        statsd_client.timing('strokes.query', int(db_query_time * 1000))

        reference_time = time.time()
        stroke_array = map(lambda stroke: [(end_time - stroke.get_timestamp()).seconds, stroke.get_x(), stroke.get_y(),
                                           stroke.get_lateral_error(), stroke.get_amplitude(),
                                           stroke.get_station_count(), stroke.get_type()], strokes)
        statsd_client.timing('strokes.reduce', int((time.time() - reference_time) * 1000))

        response = {'s': stroke_array, 't': end_time.strftime("%Y%m%dT%H:%M:%S"),
                    'h': stroke_db.select_histogram(minute_length, minute_offset, 5)}

        if strokes:
            response['next'] = long(strokes[-1].get_id() + 1)

        print 'get_strokes(%d, %d): #%d (%.2fs)' % (
            minute_length, id_or_offset, len(strokes), db_query_time)

        full_time = time.time()
        statsd_client.incr('strokes')
        statsd_client.timing('strokes', int((full_time - reference_time) * 1000))

        return response

    def jsonrpc_get_strokes_around(self, longitude, latitude, minute_length, min_id=None):
        pass

    def get_strokes_raster(self, minute_length, raster_baselength, minute_offset, region):

        stroke_db = blitzortung.db.stroke()

        end_time = datetime.datetime.utcnow()
        end_time = end_time.replace(tzinfo=pytz.UTC, microsecond=0)
        end_time += datetime.timedelta(minutes=minute_offset)

        start_time = end_time - datetime.timedelta(minutes=minute_length)
        time_interval = blitzortung.db.TimeInterval(start_time, end_time)

        raster_data = raster[region].get_for(raster_baselength)

        reference_time = time.time()
        raster_strokes = stroke_db.select_raster(raster_data, time_interval)
        statsd_client.timing('strokes_raster.query', int((time.time() - reference_time) * 1000))

        reference_time = time.time()
        reduced_stroke_array = raster_strokes.to_reduced_array(end_time)
        statsd_client.timing('strokes_raster.reduce', int((time.time() - reference_time) * 1000))

        reference_time = time.time()
        histogram = stroke_db.select_histogram(minute_length, minute_offset, 5, envelope=raster_data)
        statsd_client.timing('strokes_raster.histogram_query', int((time.time() - reference_time) * 1000))

        reference_time = time.time()
        response = {'r': reduced_stroke_array, 'xd': raster_data.get_x_div(), 'yd': raster_data.get_y_div(),
                    'x0': raster_data.get_x_min(), 'y1': raster_data.get_y_max(), 'xc': raster_data.get_x_bin_count(),
                    'yc': raster_data.get_y_bin_count(), 't': end_time.strftime("%Y%m%dT%H:%M:%S"),
                    'h': histogram}
        statsd_client.timing('strokes_raster.pack_response', int((time.time() - reference_time) * 1000))

        return response

    def jsonrpc_get_strokes_raster(self, minute_length, raster_base_length=10000, minute_offset=0, region=1):
        raster_base_length = self.__force_min(raster_base_length, 5000)
        minute_length = self.__force_range(minute_length, 0, 24 * 60)
        minute_offset = self.__force_range(minute_offset, -24 * 60 + minute_length, 0)

        reference_time = time.time()

        response = self.strokes_raster_cache.get(self.get_strokes_raster, minute_length=minute_length,
                                                 raster_baselength=raster_base_length,
                                                 minute_offset=minute_offset, region=region)

        full_time = time.time() - reference_time
        data_size = len(response['r'])

        statsd_client.incr('strokes_raster')
        statsd_client.timing('strokes_raster', int(full_time * 1000))
        statsd_client.gauge('strokes_raster.size', data_size)

        print 'get_strokes_raster(%d, %d, %d, %d): #%d (%.2fs, %.1f%%)' % (
            minute_length, raster_base_length, minute_offset, region, data_size, full_time,
            self.strokes_raster_cache.get_ratio() * 100)

        return response

    def jsonrpc_get_stations(self):
        stations_db = blitzortung.db.station()

        reference_time = time.time()
        stations = stations_db.select()
        query_time = time.time()
        statsd_client.timing('stations.query', int((query_time - reference_time) * 1000))

        station_array = []
        for station in stations:
            station_data = [station.get_number(), station.get_name(), station.get_country(), station.get_x(),
                            station.get_y()]

            if station.get_timestamp():
                station_data.append(station.get_timestamp().strftime("%Y%m%dT%H:%M:%S.%f")[:-3])
            else:
                station_data.append('')

            station_array.append(station_data)

        response = {'stations': station_array}

        full_time = time.time()

        print 'get_stations(): #%d (%.2fs)' % (len(stations), query_time - reference_time)
        statsd_client.incr('stations')
        statsd_client.timing('stations', int((full_time - reference_time) * 1000))

        return response


users = {'test': 'test'}

# Set up the application and the JSON-RPC resource.
application = service.Application("Blitzortung.org JSON-RPC Server")
logfile = DailyLogFile("webservice.log", "/var/log/blitzortung")
application.setComponent(ILogObserver, FileLogObserver(logfile).emit)
root = Blitzortung()

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
#site = server.Site(resource=wrappedRoot)
config = blitzortung.config.config()
site = server.Site(root)
site.displayTracebacks = False
jsonrpc_server = internet.TCPServer(config.get_webservice_port(), site)
jsonrpc_server.setServiceParent(application)
