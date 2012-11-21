#!/usr/bin/env python

from twisted.internet import epollreactor
epollreactor.install()

from zope.interface import Interface, implements
from twisted.cred import portal, checkers, credentials, error as credential_error
from twisted.cred.checkers import InMemoryUsernamePasswordDatabaseDontUse
from twisted.web import server, http
from twisted.web.guard import HTTPAuthSessionWrapper, DigestCredentialFactory
from twisted.internet import reactor
from twisted.application import service, internet
from twisted.python.log import ILogObserver, FileLogObserver
from twisted.python.logfile import DailyLogFile 
from twisted.python import log

from txjsonrpc.auth import wrapResource
from txjsonrpc.web import jsonrpc

import logging
import math
import datetime
import time
import pyproj
import pytz
import json

json.encoder.FLOAT_REPR = lambda f: ("%.4f" % f)

import blitzortung

WGS84 = pyproj.Proj(init='epsg:4326')
UTM_EU = pyproj.Proj(init='epsg:32633') # UTM 33 N / WGS84
UTM_USA = pyproj.Proj(init='epsg:32614') # UTM 14 N / WGS84
UTM_OC = pyproj.Proj(init='epsg:32755') # UTM 55 S / WGS84

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
  ' should have attribute username '
 
class UserAvatar(object):
  implements(IUserAvatar)

  def __init__(self, username):
    self.username = username


class TestRealm(object):
  implements(portal.IRealm)

  def __init__(self, users):
    self.users = users

  def requestAvatar(self, avatarId, mind, *interfaces):
    if INamedUserAvatar in interfaces:
      logout = lambda: None
      return (IUserAvatar,
	  UserAvatar(avatarId),
	  logout)
    else:
      raise KeyError('none of the requested interfaces is supported')


class RasterData(object):
  
  def __init__(self, min_lon, max_lon, min_lat, max_lat, coord_sys):
    self.min_lon = min_lon
    self.max_lon = max_lon
    self.min_lat = min_lat
    self.max_lat = max_lat
    self.coord_sys = coord_sys

    self.raster_data = {}

  def fix_max(self, minimum, maximum, delta):
    return  minimum + math.floor((maximum - minimum)/delta)*delta

  def get_for(self, baselength):
    if baselength not in self.raster_data:
	ref_lon = (self.min_lon + self.max_lon) / 2.0
	ref_lat = (self.min_lat + self.max_lat) / 2.0

	utm_x, utm_y = pyproj.transform(WGS84, self.coord_sys, ref_lon, ref_lat)
	lon_d, lat_d = pyproj.transform(self.coord_sys, WGS84, utm_x + baselength, utm_y + baselength)

	delta_lon = lon_d - ref_lon
	delta_lat = lat_d - ref_lat

	max_lon = self.fix_max(self.min_lon, self.max_lon, delta_lon)
	max_lat = self.fix_max(self.min_lat, self.max_lat, delta_lat)

	self.raster_data[baselength] = blitzortung.geom.Raster(self.min_lon, max_lon, self.min_lat, max_lat, delta_lon, delta_lat, blitzortung.geom.Geometry.DefaultSrid)

    return self.raster_data[baselength]


raster = {}

raster[1] = RasterData(-12, 35, 35, 65, UTM_EU)
raster[2] = RasterData(140, 180, -50, -10, UTM_OC)
raster[3] = RasterData(-140, -50, 10, 60, UTM_USA)

class Blitzortung(jsonrpc.JSONRPC):
  """
  An example object to be published.
  """

  addSlash = True

  def __force_min(self, number, min_number):
    return max(min_number, number)

  def __force_max(self, number, max_number):
    return min(max_number, number)

  def __force_range(self, number, min_number, max_number):
    return self.__force_min(self.__force_max(number, max_number), min_number)

  def jsonrpc_get_strokes(self, minute_length, min_id=None):
    minute_length = self.__force_range(minute_length, 0, 24*60)

    strokedb = blitzortung.db.stroke()

    endtime = datetime.datetime.utcnow()
    endtime = endtime.replace(tzinfo=pytz.UTC)
    endtime = endtime.replace(microsecond = 0)
    
    starttime = endtime - datetime.timedelta(minutes=minute_length)
    time_interval = blitzortung.db.TimeInterval(starttime, endtime)

    if min_id is not None:
      id_interval = blitzortung.db.IdInterval(min_id)
    else:
      id_interval = None
      min_id = 0

    area = None
    order = blitzortung.db.Order('id')

    reference_time = time.time()

    strokes = strokedb.select(time_interval, id_interval, area, order)

    query_time = time.time()

    max_id = None
    stroke_array = []
    for stroke in strokes:
      stroke_data = []

      timestamp = stroke.get_timestamp()
      stroke_data.append(((endtime - stroke.get_timestamp()).seconds))
      stroke_data.append(stroke.get_x())
      stroke_data.append(stroke.get_y())
      stroke_data.append(stroke.get_lateral_error())
      stroke_data.append(stroke.get_amplitude())
      stroke_data.append(stroke.get_station_count())
      stroke_data.append(stroke.get_type())

      stroke_array.append(stroke_data)
      max_id = stroke.get_id()
  
    response = {}
    response['s'] = stroke_array
    response['t'] = endtime.strftime("%Y%m%dT%H:%M:%S")

    if max_id:
      response['next'] = long(max_id + 1)

    print 'get_strokes(%d, %d): #%d (%.2fs)' %(minute_length, min_id, len(strokes), query_time - reference_time)

    return response

  def jsonrpc_get_strokes_around(self, longitude, latitude, minute_length, min_id=None):
    pass

  def jsonrpc_get_strokes_raster(self, minute_length, raster_baselength=10000, minute_offset=0, region=1):
    raster_baselength = self.__force_min(raster_baselength, 5000)
    minute_length = self.__force_range(minute_length, 0, 24 * 60)
    minute_offset = self.__force_range(minute_offset, -24 * 60 + minute_length, 0)

    strokedb = blitzortung.db.stroke()

    endtime = datetime.datetime.utcnow()
    endtime = endtime.replace(microsecond = 0)
    endtime += datetime.timedelta(minutes=minute_offset)

    starttime = endtime - datetime.timedelta(minutes=minute_length)
    time_interval = blitzortung.db.TimeInterval(starttime, endtime)

    reference_time = time.time()

    raster_data = raster[region].get_for(raster_baselength)

    raster_strokes = strokedb.select_raster(raster_data, time_interval)
    histogram = strokedb.select_histogram(minute_length, minute_offset, region, 5)

    query_time = time.time()

    endtime = endtime.replace(tzinfo=pytz.UTC)
    reduced_stroke_array = raster_strokes.to_reduced_array(endtime)

    response = {}
    response['r'] = reduced_stroke_array
    response['xd'] = raster_data.get_x_div()
    response['yd'] = raster_data.get_y_div()
    response['x0'] = raster_data.get_x_min()
    response['y1'] = raster_data.get_y_max()
    response['xc'] = raster_data.get_x_bin_count()
    response['yc'] = raster_data.get_y_bin_count()
    response['t'] = endtime.strftime("%Y%m%dT%H:%M:%S")
    response['h'] = histogram

    print 'get_strokes_raster(%d, %d, %d, %d): #%d (%.2fs)' %(minute_length, raster_baselength, minute_offset, region, len(reduced_stroke_array), query_time - reference_time)

    return response

  def jsonrpc_get_stations(self):
    stationsdb = blitzortung.db.station()

    reference_time = time.time()
    stations = stationsdb.select()
    query_time = time.time()

    station_array = []
    for station in stations:
      station_data = []

      station_data.append(station.get_number())
      station_data.append(station.get_location_name())
      station_data.append(station.get_country())
      station_data.append(station.get_x())
      station_data.append(station.get_y())
      if station.get_timestamp():
        station_data.append(station.get_timestamp().strftime("%Y%m%dT%H:%M:%S.%f")[:-3])
      else:
	station_data.append('')

      station_array.append(station_data)
  
    response = {'stations': station_array}

    print 'get_stations(): #%d (%.2fs)' %(len(stations), query_time - reference_time)

    return response


users = {'test':'test'}

# Set up the application and the JSON-RPC resource.
application = service.Application("Blitzortung.org JSON-RPC Server")
logfile = DailyLogFile("webservice.log", "/var/log/blitzortung")
application.setComponent(ILogObserver, FileLogObserver(logfile).emit)
root = Blitzortung()

credentialFactory = DigestCredentialFactory("md5", "blitzortung.org")
# Define the credential checker the application will be using and wrap the JSON-RPC resource.
checker = InMemoryUsernamePasswordDatabaseDontUse()
checker.addUser('test','test')
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
jsonrpcServer = internet.TCPServer(config.get_webservice_port(), site)
jsonrpcServer.setServiceParent(application)
