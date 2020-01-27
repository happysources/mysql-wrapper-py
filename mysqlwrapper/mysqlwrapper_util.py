#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Utils for MySQL wrapper
"""

import time


def _sql_now():
	""" now() """

	return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))


def _sql_column(column_list):
	""" sql columns """

	if not column_list:
		return ' * '

	ret = ','.join(column_list)
	return ret

def _sql_value(value_dict):
	""" sql NAME=VALUE """

	name_values = []
	params = []

	for value_name in value_dict:
		name_values.append('`%s`=%%s' % value_name)
		params.append(value_dict[value_name])

	return name_values, params


def _sql_set(value_dict):
	""" sql SET """

	if not value_dict:
		return '', []

	sets, params = _sql_value(value_dict)

	sql_set = ', '.join(sets)

	return sql_set, params


def _sql_where(where_dict):
	""" sql WHERE """

	# sql where
	if not where_dict:
		return '', []

	wheres, params = _sql_value(where_dict)

	sql_where = ' WHERE %s ' % ' AND '.join(wheres)

	return sql_where, params


def _sql_limit(limit=0):
	""" sql LIMIT """

	# sql limit
	if not limit:
		return ''

	sql_limit = ' LIMIT %s ' % limit

	return sql_limit


def _debug(msg):
	""" debug message """

	#if self.__debug_level:
	print(msg)
