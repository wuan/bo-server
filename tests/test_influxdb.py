import calendar
import json
import unittest

import datetime
from assertpy import assert_that

from blitzortung_server.influxdb import DataPoint


class TestAS(unittest.TestCase):
    def testEmptyDataPoint(self):
        timestamp = calendar.timegm(datetime.datetime.utcnow().timetuple())
        data_point = DataPoint("meas")

        json_repr = data_point.get()
        assert_that(json.loads(json_repr)).is_equal_to({
            "measurement": "meas",
            "timestamp": timestamp,
            "tags": {},
            "fields": {}
        })

    def testDataPoint(self):
        timestamp = calendar.timegm(datetime.datetime.utcnow().timetuple())
        data_point = DataPoint("meas")
        data_point.fields['foo'] = 1.5
        data_point.tags['bar'] = "qux"
        data_point.tags['baz'] = 1234

        json_repr = data_point.get()
        assert_that(json.loads(json_repr)).is_equal_to({
            "measurement": "meas",
            "timestamp": timestamp,
            "tags": {'bar': 'qux', 'baz': 1234},
            "fields": {'foo': 1.5}
        })
