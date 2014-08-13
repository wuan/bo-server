from __future__ import print_function
import psycopg2
import psycopg2.extras
import time
import pytz
from twisted.python import log

from txpostgres import reconnection
from txpostgres.txpostgres import ConnectionPool, Connection
import blitzortung.config
import blitzortung.builder
import blitzortung.db.query


def connection_factory(*args, **kwargs):
    print("create connection", " ".join(args))
    kwargs['connection_factory'] = psycopg2.extras.DictConnection
    return psycopg2.connect(*args, **kwargs)


class DictConnection(Connection):
    connectionFactory = staticmethod(connection_factory)


class DictConnectionPool(ConnectionPool):
    connectionFactory = DictConnection

    def __init__(self, _ignored, *connargs, **connkw):
        super(DictConnectionPool, self).__init__(_ignored, *connargs, **connkw)


def create_connection_pool():
    config = blitzortung.config.config()
    db_connection_string = config.get_db_connection_string()

    connection_pool = DictConnectionPool(None, db_connection_string)

    print(connection_pool.connectionFactory)
    d = connection_pool.start()
    d.addErrback(log.err)
    return connection_pool


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


class QueryBuilder(object):
    @staticmethod
    def select_strokes_query(*args, **kwargs):
        print("select_strokes_query()")

        """ build up query object for select statement """

        query = kwargs['query'] if 'query' in kwargs else blitzortung.db.query.Query()

        query.set_table_name("strikes")
        query.set_columns(['id', '"timestamp"', 'nanoseconds', 'ST_X(ST_Transform(geog::geometry, %(srid)s)) AS x',
                           'ST_Y(ST_Transform(geog::geometry, %(srid)s)) AS y', 'altitude', 'amplitude', 'error2d',
                           'stationcount'])
        query.add_parameters({'srid': 4326})

        query.parse_args(args)

        return query


def strike_result_build(query_result, end_time, statsd_client, reference_time):
    print("strike_result_build()")
    print("  end_time", end_time)
    query_time = time.time()
    db_query_time = (query_time - reference_time)
    statsd_client.timing('strikes.query', max(1, int(db_query_time * 1000)))

    reference_time = time.time()
    strike_builder = blitzortung.builder.Strike()
    strikes = tuple(
        (
            (end_time - strike.get_timestamp()).seconds,
            strike.get_x(),
            strike.get_y(),
            strike.get_altitude(),
            strike.get_lateral_error(),
            strike.get_amplitude(),
            strike.get_station_count()
        ) for strike in create_strikes(query_result, strike_builder))

    result = {'s': strikes}

    if strikes:
        result['next'] = query_result[-1][0] + 1

    statsd_client.timing('strikes.reduce', max(1, int((time.time() - reference_time) * 1000)))
    return result


def create_strikes(query_results, strike_builder):
    print("create_strikes()")
    for result in query_results:
        yield create_object_instance(strike_builder, result)


def create_object_instance(strike_builder, result):
    strike_builder.set_id(result['id'])
    strike_builder.set_timestamp(result['timestamp'].astimezone(pytz.UTC), result['nanoseconds'])
    strike_builder.set_x(result['x'])
    strike_builder.set_y(result['y'])
    strike_builder.set_altitude(result['altitude'])
    strike_builder.set_amplitude(result['amplitude'])
    strike_builder.set_station_count(result['stationcount'])
    strike_builder.set_lateral_error(result['error2d'])

    return strike_builder.build()


def compile_strikes_result(result, end_time):
    strikes_result = result[0]
    histogram_result = result[1]

    base_result = {'t': end_time.strftime("%Y%m%dT%H:%M:%S"), 'h': histogram_result}
    base_result.update(strikes_result)
    return base_result


def histogram_result_build(cursor, minutes, bin_size, reference_time):
    time_duration = time.time() - reference_time
    print("histogram_query() %.03fs" % time_duration)
    value_count = minutes / bin_size

    result = [0] * value_count

    for bin_data in cursor:
        result[bin_data[0] + value_count - 1] = bin_data[1]

    return result




