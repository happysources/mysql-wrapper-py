#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Utils for MySQL wrapper
"""

import time


def _sql_now():
	""" now() """

	return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))


def _sql_column(column_list, table_name=None):
	""" sql columns """

	if not column_list:
		return ' * '

	columns = []

	# type: dict
	if type(column_list) == type({}):
		for column_name in column_list.keys():
			as_name = column_list[column_name]

			if column_name.startswith('date_') or column_name.endswith('_date'):
				columns.append('UNIX_TIMESTAMP(`%s`) AS `%s`' % (column_name, as_name))
			else:
				columns.append('`%s` AS `%s`' % (column_name, as_name))

		ret = ', '.join(columns)
		return ret

	# type: tuple/array
	for column_name in column_list:

		if column_name == 'id' and table_name:
			columns.append('`id` AS %s_id' % table_name)
		elif column_name.startswith('date_') or column_name.endswith('_date'):
			columns.append('UNIX_TIMESTAMP(`%s`) AS `%s`' % (column_name, column_name))
		else:
			columns.append('`%s`' % column_name)

	ret = ', '.join(columns)
	return ret


def _sql_value(value_dict):
	""" sql NAME=VALUE """

	name_values = []
	params = []

	for value_name in value_dict:
		value_val = value_dict[value_name]

		if type(value_val) == type([]):
			name_values.append('`%s` IN %%s' % value_name)
		else:
			name_values.append('`%s`=%%s' % value_name)

		params.append(value_val)
	
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
