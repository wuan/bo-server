import time
from injector import inject
from twisted.internet.defer import gatherResults
from twisted.python import log

import blitzortung


class StrikeQuery(object):

    @inject(stike_query_builder=blitzortung.db.query_builder.Strike, strike_mapper=blitzortung.db.mapper.Strike)
    def __init__(self, strike_query_builder, strike_mapper):
        self.strike_query_builder = strike_query_builder
        self.strike_mapper = strike_mapper

    def create(self, id_or_offset, minute_length, minute_offset, reference_time, connection, statsd_client):
        query, end_time = self.create_strike_query(id_or_offset, minute_length, minute_offset)
        strikes_query = connection.runQuery(str(query), query.get_parameters())
        strikes_query.addCallback(self.strike_result_build, end_time=end_time,
                                  statsd_client=statsd_client, reference_time=reference_time)
        return strikes_query, end_time

    def create_strike_query(self, id_or_offset, minute_length, minute_offset):
        time_interval = self.create_time_interval(minute_length, minute_offset)

        if id_or_offset > 0:
            id_interval = blitzortung.db.query.IdInterval(id_or_offset)
        else:
            id_interval = None

        order = blitzortung.db.query.Order('id')

        return self.strike_query_builder.select_query(blitzortung.db.table.Strike.TABLE_NAME,
                                                      blitzortung.geom.Geometry.DefaultSrid, time_interval,
                                                      id_interval, order), time_interval.get_end()

    def strike_result_build(self, query_result, end_time, statsd_client, reference_time):
        time_duration = time.time() - reference_time
        print("strike_result_build() %.03fs" % time_duration)
        statsd_client.timing('strikes.query', max(1, int(time_duration * 1000)))

        reference_time = time.time()
        strikes = tuple(
            (
                (end_time - strike.get_timestamp()).seconds,
                strike.get_x(),
                strike.get_y(),
                strike.get_altitude(),
                strike.get_lateral_error(),
                strike.get_amplitude(),
                strike.get_station_count()
            ) for strike in self.create_strikes(query_result))

        result = {'s': strikes}

        if strikes:
            result['next'] = query_result[-1][0] + 1

        statsd_client.timing('strikes.reduce', max(1, int((time.time() - reference_time) * 1000)))
        return result

    def create_strikes(self, query_results):
        print("create_strikes()")
        for result in query_results:
            yield self.strike_mapper.create_object(result)

    def combine_result(self, strikes_query, histogram_query, reference_time, statsd_client):
        query = gatherResults([strikes_query, histogram_query], consumeErrors=True)
        query.addCallback(self.compile_strikes_result, reference_time=reference_time, end_time=end_time)
        query.addErrback(log.err)

    @staticmethod
    def compile_strikes_result(result, reference_time, end_time):
        time_duration = time.time() - reference_time
        print("compile_strikes_result() %.03fs" % time_duration)
        statsd_client.timing('strikes.result', max(1, int(time_duration * 1000)))

        strikes_result = result[0]
        histogram_result = result[1]

        base_result = {'t': end_time.strftime("%Y%m%dT%H:%M:%S"), 'h': histogram_result}
        base_result.update(strikes_result)

        return base_result


