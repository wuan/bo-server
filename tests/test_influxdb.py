import datetime
import unittest

from assertpy import assert_that

from blitzortung_server.influxdb import DataPoint


class TestAS(unittest.TestCase):
    def testEmptyDataPoint(self):
        timestamp = datetime.datetime.utcnow()
        data_point = DataPoint("meas", time=timestamp)

        json_repr = data_point.get()
        assert_that(json_repr).is_equal_to({
            "measurement": "meas",
            "time": timestamp,
            "tags": {},
            "fields": {}
        })

    def testDataPoint(self):
        timestamp = datetime.datetime.utcnow()
        data_point = DataPoint("meas", time=timestamp)
        data_point.fields['foo'] = 1.5
        data_point.tags['bar'] = "qux"
        data_point.tags['baz'] = 1234

        json_repr = data_point.get()
        assert_that(json_repr).is_equal_to({
            "measurement": "meas",
            "time": timestamp,
            "tags": {'bar': 'qux', 'baz': 1234},
            "fields": {'foo': 1.5}
        })
