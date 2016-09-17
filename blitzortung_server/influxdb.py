import calendar
import datetime


class DataPoint(object):
    def __init__(self, measurement, timestamp=None, fields=None, tags=None):
        self.measurement = measurement
        if timestamp is None:
            timestamp = datetime.datetime.utcnow()
        self.time = calendar.timegm(timestamp.timetuple())
        self.fields = fields if fields is not None else {}
        self.tags = tags if tags is not None else {}

    def get(self):
        return self.__dict__
