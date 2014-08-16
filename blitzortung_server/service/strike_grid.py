from injector import inject
import time
from twisted.internet.defer import gatherResults
from twisted.python import log

import blitzortung

from . import create_time_interval


class StrikeGridQuery(object):
    @inject(strike_query_builder=blitzortung.db.query_builder.Strike)
    def __init__(self, strike_query_builder):
        self.strike_query_builder = strike_query_builder

    def create(self, grid_parameters, minute_length, minute_offset, reference_time, connection, statsd_client):
        time_interval = create_time_interval(minute_length, minute_offset)

        query = self.strike_query_builder.grid_query(blitzortung.db.table.Strike.TABLE_NAME, grid_parameters,
                                                     time_interval)
        grid_query = connection.runQuery(str(query), query.get_parameters())
        grid_query.addCallback(self.build_strikes_grid_result, end_time=time_interval.get_end(),
                               statsd_client=statsd_client, reference_time=reference_time,
                               grid_parameters=grid_parameters)
        grid_query.addErrback(log.err)
        return grid_query, time_interval.get_end()

    @staticmethod
    def build_strikes_grid_result(results, end_time, statsd_client, reference_time, grid_parameters):
        query_duration = time.time() - reference_time
        print("strikes_grid_query() %.03fs #%d %s" % (query_duration, len(results), grid_parameters))
        statsd_client.timing('strikes_grid.query', max(1, int(query_duration * 1000)))

        reference_time = time.time()
        y_bin_count = grid_parameters.get_y_bin_count()
        strikes_grid_result = tuple(
            (
                result['rx'],
                y_bin_count - result['ry'] - 1,
                result['count'],
                -(end_time - result['timestamp']).seconds
            ) for result in results
        )
        statsd_client.timing('strikes_grid.build_result', max(1, int((time.time() - reference_time) * 1000)))

        return strikes_grid_result

    def combine_result(self, strike_grid_result, histogram_result):

        combined_result = gatherResults([strike_grid_result, histogram_result], consumeErrors=True)
        combined_result.addCallback(self.build_grid_response, grid_parameters=grid_parameters, end_time=end_time,
                          reference_time=reference_time)
        combined_result.addErrback(log.err)

        return combined_result

    @staticmethod
    def build_grid_response(results, end_time, grid_parameters, reference_time):
        time_duration = time.time() - reference_time
        print("build_grid_response() %.03fs" % time_duration)
        statsd_client.timing('strikes_grid.result', max(1, int(time_duration * 1000)))

        grid_data = results[0]
        histogram_data = results[1]

        statsd_client.gauge('strikes_grid.size', len(grid_data))
        statsd_client.incr('strikes_grid')

        reference_time = time.time()
        response = {'r': grid_data, 'xd': round(grid_parameters.get_x_div(), 6),
                    'yd': round(grid_parameters.get_y_div(), 6),
                    'x0': round(grid_parameters.get_x_min(), 4), 'y1': round(grid_parameters.get_y_max(), 4),
                    'xc': grid_parameters.get_x_bin_count(),
                    'yc': grid_parameters.get_y_bin_count(), 't': end_time.strftime("%Y%m%dT%H:%M:%S"),
                    'h': histogram_data}
        statsd_client.timing('strikes_grid.pack_response', max(1, int((time.time() - reference_time) * 1000)))
        return response

