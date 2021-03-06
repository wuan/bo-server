import datetime


class DataPoint(object):
    def __init__(self, measurement, time=None, fields=None, tags=None):
        self.measurement = measurement
        self.time = time if time is not None else datetime.datetime.utcnow()
        self.fields = fields if fields is not None else {}
        self.tags = tags if tags is not None else {}

    def get(self):
        return {
            'measurement': self.measurement,
            'time': self.time,
            'tags': self.tags,
            'fields': self.fields
        }
