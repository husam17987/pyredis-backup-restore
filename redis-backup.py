#!/usr/bin/env python
import argparse
import redis
import sys

def connect_redis(conn_dict):
    conn = redis.StrictRedis(host=conn_dict['host'],
                             port=conn_dict['port'],
                             db=conn_dict['db'])
    return conn


def conn_string_type(string):
    format = '<host>:<port>/<db>'
    try:
        host, portdb = string.split(':')
        port, db = portdb.split('/')
        db = int(db)
    except ValueError:
        raise argparse.ArgumentTypeError('incorrect format, should be: %s' % format)
    return {'host': host,
            'port': port,
            'db': db}

def migrate_redis(source, destination):
    src = connect_redis(source)
    dst = connect_redis(destination)
    first = True
    cursor = '0'
    while cursor !='0' or first:
	cursor, data = r.execute_command('scan', cursor)
	first = False
	for key in data:
		ttl = src.ttl(key)
		if ttl < 0:
			ttl = 0
		value = src.dump(key)
		if src.type(key) == 'none':
			continue
		if not dst.exists(key):
			print "Restoring key: %s" % key
			dst.restore(key, ttl * 1000, value)
    return

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('source', type=conn_string_type)
    parser.add_argument('destination', type=conn_string_type)
    #parser.add_argument('db', type=conn_string_type)
    options = parser.parse_args()
    migrate_redis(options.source, options.destination)


if __name__ == '__main__':
    run()
