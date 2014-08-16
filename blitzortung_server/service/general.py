import datetime
import pytz
import blitzortung


def create_time_interval(minute_length, minute_offset):
    end_time = datetime.datetime.utcnow()
    end_time = end_time.replace(tzinfo=pytz.UTC)
    end_time = end_time.replace(microsecond=0)
    end_time += datetime.timedelta(minutes=minute_offset)
    start_time = end_time - datetime.timedelta(minutes=minute_length)
    time_interval = blitzortung.db.query.TimeInterval(start_time, end_time)
    return time_interval

