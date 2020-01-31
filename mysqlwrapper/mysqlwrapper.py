#!/usr/bin/python3
# -*- coding: utf-8 -*-

# todo:
# pylint: disable=no-member
# pylint: disable=protected-access

"""
MySQL wrapper
"""

import time

from mysqlwrapper_util import _sql_where, _sql_column, _sql_limit, _sql_set
from logni import log


try:
	import MySQLdb
except ImportError as import_err:
	log.error('run $ pip install -r requirements.txt, please [err="%s"]', import_err, 2)
	#sys.exit(1)


class Connect(object):
	""" MySQLwrapper Connect object """

	def __init__(self, user='root', passwd='', db='', host='localhost', param=None):
		""" Connect initialize

		Parameters:
			user (str): mysql user
			passwd (str): mysql password
			db (str): mysql database
			host (str): mysql hostname or ip
			param (dict): mysql parameters for connect """

		if not param:
			param = {}

		# input params
		self._param = {\
			'host':host,\
			'db':db,\
			'user':user,\
			'passwd':passwd,\
			'port':param.get('port', 3306),\
			'connect_timeout':param.get('connect_timeout', 5),\
			'dict_cursor':param.get('dict_cursor', 1),\
			'charset':param.get('charset', 'utf8'),\
			'autocommit':param.get('autocommit', 1),\
			'dummy':param.get('dummy', 1),\
			'debug':param.get('debug', 0),\
		}

		# default
		self.__share = {'dbh':None}

		#if not self.__conv:
		#	from MySQLdb.converters import conversions
		#	conversions = conversions.copy()
		#	conversions[Fixed] = conv_fixed
		#	conversions[types.ListType]  = conv_array
		#	self.__conv = conversions

		# connect
		self.__connect_time = 0
		self._connect()


	def __getattr__(self, name):
		return getattr(self.__share['dbh'], name)


	def _connect(self):
		""" connect """

		start_time = time.time()
		dbh = None
		self.__share = {'dbh':dbh}

		# connect
		try:
			dbh = MySQLdb.Connect(\
				host=self._param['host'],\
				db=self._param['db'],\
				user=self._param['user'],\
				passwd=self._param['passwd'],\
				port=self._param['port'],\
				connect_timeout=self._param['connect_timeout'])
			self.__connect_time = time.time() - start_time

		except BaseException as emsg:
			log.error('mysql host=%s:%s, user=%s, connect error=%s',\
				(self._param['host'], self._param['port'], self._param['user'],\
				emsg), 2)
			if self._param.get('dummy', 1):
				log.warn('mysql host=%s:%s, user=%s, no connect',\
					(self._param['host'], self._param['port'],\
					self._param['user'], emsg), 2)
				return -1

			raise

		# set autocommit an charset
		dbh.autocommit(self._param['autocommit'])
		dbh.set_character_set(self._param['charset'])

		# connect time
		self.__connect_time = time.time() - start_time

		self.__share['dbh'] = dbh

		log.info('mysql host=%s:%s, user=%s, connect OK',\
			(self._param['host'], self._param['port'], self._param['user']), 2)


	def _dbh(self):
		""" dbh """
		return self.__share['dbh']


	def cursor(self, dict_cursor=None):
		""" mysql cursor """

		if not dict_cursor:
			dict_cursor = self._param['dict_cursor']

		return self.Cursor(dbh=self, dict_cursor=dict_cursor, debug=self._param['debug'])


	def close(self):
		""" close """

		try:
			self.__share['dbh'].close()
		except BaseException as emsg:
			log.error('mysql Exception, database close err="%s"', emsg, 2)



	class Cursor(object):
		""" MySQLwrapper Cursor object """

		def __init__(self, dbh=None, dict_cursor=None, debug=0):
			""" Cursor initialize

			Parameters:
				dbh (object): database connect
				dict_cursor (int): dictionary cursor """

			# input params
			self.__dbh = dbh
			self.__dict_cursor = dict_cursor
			self.__debug_level = debug

			self._param = dbh._param
			self.__share = {'dbh':dbh}

			# default
			self.__cursor = None

			# create cursor
			self.__create()


		#def __getattr__(self, name):
		#	return getattr(self.__cursor, name)


		def __debug(self, msg):
			""" debug message """
			if self.__debug_level:
				log.debug('mysql debug=%s', msg, 1)


		def __create(self):
			""" create cursor """

			log.debug('create() __dbh dir: %s', dir(self.__dbh), 1)

			if not self.__dbh._dbh():
				log.error('mysql cursor: no connect', (), 2)
				return False

			if self.__dict_cursor:
				self.__cursor = self.__dbh._dbh().cursor(MySQLdb.cursors.DictCursor)
				return True

			self.__cursor = self.__dbh._dbh().cursor()
			return True


		def __test(self):
			""" tested cursor if exist? """

			# reconnect id cursor not exist
			if not self.__cursor:
				ret_connect = self.__dbh._connect()
				if ret_connect == -1:
					log.error('MySQLdb.InterfaceError: No connect for cursor [first connect]', (), 2)
					raise MySQLdb.InterfaceError(0, 'No connect for cursor [first connect]')

				self.__cursor()

			return True


		def execute(self, query='', param=()):
			""" cursor execute """

			# default
			found = 0

			# test if cursor exist?
			self.__test()

			# sql execute
			start_time = time.time()

			log.debug('execute sql param: %s', str(param), 3)
			log.debug('execute sql query: %s', query, 3)

			__query = query % tuple(param)
			log.debug('execute sql: %s', __query, 4)

			try:
				found = self.__cursor.execute(query, param)

			except MySQLdb.OperationalError as mysql_error:
				log.error('MySQLdb.OperationalError, err="%s"', mysql_error, 2)
				ret_connect = Connect._connect(self)
				if ret_connect == -1:
					raise MySQLdb.InterfaceError(0, 'No connect for cursor, err="%s"' % mysql_error)

				found = self.__cursor.execute(query, param)

			except BaseException as emsg:
				log.error('mysql Exception, execute err="%s"', emsg, 2)
				raise

			run_time = time.time() - start_time

			log.info('execute query: time=%.3fs found=%s', (run_time, found), 2)
			return found


		def __fetch(self, fetch_type='all'):
			""" fetch """

			start_time = time.time()

			if fetch_type == 'one':
				ret = self.__cursor.fetchone()
			else:
				ret = self.__cursor.fetchall()

			run_time = time.time() - start_time

			log.info('fetch%s query: time=%.3fs', (fetch_type, run_time,), 2)
			return ret


		def fetchall(self):
			""" cursor fetchall """
			return self.__fetch('all')


		def fetchone(self):
			""" cursor fetchone """
			return self.__fetch('one')

		def insert_id(self):
			""" last insert_id """
			return self.__dbh.insert_id()

		def close(self):
			""" cursor close """

			try:
				self.__cursor.close()

			except BaseException as emsg:
				log.error('mysql Exception, cursor close err="%s"', emsg, 2)


		def select(self, table_name, where_dict, column_list=(), limit=0):
			""" Simple SELECT

			Parameters:
				table_name (str): table name
				where_dict (dict): sql where
				limit (int): sql limit
				column_list (array): colum names

			Returns:
				found (int): row number
				data (array): data from fetchall() """

			if not table_name:
				log.error('select: table_name must be input', (), 2)
				return 0, None

			(sql_where, sql_param) = _sql_where(where_dict)

			found = self.execute('SELECT %s FROM %s %s %s' % (\
				_sql_column(column_list, table_name), table_name,\
				sql_where, _sql_limit(limit)),\
				sql_param)

			if found == 0:
				return found, None

			return found, self.__cursor.fetchall()


		def insert(self, table_name, value_dict):
			""" Simple INSERT """

			if not table_name or not value_dict:
				log.error('insert: table_name/value_dict must be input', (), 2)
				return 0

			(sql_set, sql_param) = _sql_set(value_dict)

			found = self.execute('INSERT INTO %s SET %s' % (\
				table_name, sql_set),\
				sql_param)

			return found


		def update(self, table_name, value_dict, where_dict, limit=0):
			""" Simple UPDATE """

			if not table_name and not value_dict:
				log.error('update: table_name/value_dict must be input', (), 2)
				return 0

			(sql_set, sql_param) = _sql_set(value_dict)
			(sql_where, params2) = _sql_where(where_dict)

			for param_name in params2:
				sql_param.append(param_name)

			found = self.execute('UPDATE %s SET %s %s %s' % (\
				table_name, sql_set, sql_where, _sql_limit(limit)),\
				sql_param)

			return found


		def delete(self, table_name, where_dict=None, limit=0):
			""" Simple DELETE """

			if not table_name:
				log.error('delete: table_name must be input', (), 2)
				return 0

			if not where_dict:
				where_dict = {}

			(sql_where, sql_param) = _sql_where(where_dict)

			found = self.execute('DELETE FROM %s %s %s' % (\
				table_name, sql_where, _sql_limit(limit)),\
				sql_param)

			return found
