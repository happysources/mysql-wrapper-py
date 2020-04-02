#!/usr/bin/python3
# -*- coding: utf-8 -*-

# todo:
# pylint: disable=no-member
# pylint: disable=protected-access

"""
MySQL wrapper
"""

import time

from mysqlwrapper_util import _sql_where, _sql_column, _sql_limit, _sql_set, _sql_now
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
			'name':'',\
		}

		# default
		self._share = {'dbh':None}

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
		return getattr(self._share['dbh'], name)


	def _connect(self):
		""" connect """
		start_time = time.time()

		# name
		if not self._param['name']:
			self._param['name'] = '%s.%s' % \
				(self._param['host'], self._param['db'])
		self._name = self._param['name']

		# dbh
		dbh = None
		self._share = {'dbh':dbh}

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
			log.error('mysql INIT host=%s:%s, user=%s, connect error=%s',\
				(self._param['host'], self._param['port'], self._param['user'],\
				emsg), priority=2)
			if self._param.get('dummy', 1):
				log.warn('mysql INIT host=%s:%s, user=%s, no connect',\
					(self._param['host'], self._param['port'],\
					self._param['user']), priority=2)
				return False

			raise

		# set autocommit an charset
		dbh.autocommit(self._param['autocommit'])
		dbh.set_character_set(self._param['charset'])

		# connect time
		self.__connect_time = int((time.time() - start_time) * 1000)

		self._share['dbh'] = dbh

		log.info('mysql INIT host=%s:%s, user=%s -> name=%s, thread_id=%s, time=%sms, connect OK',\
			(self._param['host'], self._param['port'], self._name,\
			self._param['user'], dbh.thread_id(), self.__connect_time), priority=2)


		return True

	def _dbh(self):
		""" dbh """
		return self._share['dbh']


	def cursor(self, dict_cursor=None):
		""" mysql cursor """

		if not dict_cursor:
			dict_cursor = self._param['dict_cursor']

		return self.Cursor(dbh=self,\
			dict_cursor=dict_cursor,\
			name=self._name,\
			debug=self._param['debug'])


	def thread_id(self):
		""" thread id """
		return self._share['dbh'].dbh.thread_id()


	def commit(self):
		""" commit """

		try:
			self._share['dbh'].commit()
		except BaseException as emsg:
			log.error('mysql %s commit: err="%s"',\
				(self._name, emsg), priority=2)
			return False

		log.info('mysql %s commit', self._name, priority=2)
		return True


	def close(self):
		""" close """

		try:
			self._share['dbh'].close()
		except BaseException as emsg:
			log.error('mysql %s close: err="%s"',\
				(self._name, emsg), priority=2)
			return False

		log.info('mysql %s close', self._name, priority=2)
		return True


	class Cursor(object):
		""" MySQLwrapper Cursor object """

		def __init__(self, dbh=None, dict_cursor=None, name='', debug=0):
			""" Cursor initialize

			Parameters:
				dbh (object): database connect
				dict_cursor (int): dictionary cursor """

			log.debug('cursor.__init__(dbh=%s, dict_cursor=%s, name=%s, debug=%s)',\
				(dbh, dict_cursor, name, debug))

			# input params
			self.__dbh = dbh
			self.__dict_cursor = dict_cursor
			self.__name = name
			self.__debug_level = debug

			self._param = dbh._param
			self._share = {'dbh':dbh}

			# default
			self.__cursor = None
			self.__thread_id = 0

			# create cursor
			self.__create()


		#def __getattr__(self, name):
		#	return getattr(self.__cursor, name)


		def __debug(self, msg):
			""" debug message """
			if self.__debug_level:
				log.debug('mysql %s debug=%s', (self.__name, msg), priority=1)


		def __create(self):
			""" create cursor """

			log.debug('cursor.create()')

			if not self.__dbh._dbh():
				log.error('mysql %s cursor: no connect', (self.__name,), priority=2)
				return False

			if self.__dict_cursor:
				self.__thread_id = self.__dbh._dbh().thread_id()
				log.info('mysql %s dict cursor: create -> dbh.thread_id=%s',\
					(self.__name, self.__thread_id), priority=1)
				self.__cursor = self.__dbh._dbh().cursor(MySQLdb.cursors.DictCursor)
				return True

			self.__thread_id = self.__dbh._dbh().thread_id()
			log.error('mysql %s tuple cursor: create -> dbh.thread_id=%s',\
				(self.__name, self.__thread_id), priority=1)
			self.__cursor = self.__dbh._dbh().cursor()
			return True


		def __test(self):
			""" tested cursor if exist? """

			log.debug('cursor.__test()')

			# reconnect id cursor not exist
			if not self.__cursor:
				ret_connect = self.__dbh._connect()
				log.debug('mysql %s reconnect is %s', (self.__name, ret_connect), priority=4)
				if not ret_connect:
					log.error('mysql %s connect: No connect for cursor [first connect]',\
						(self.__name,), priority=2)
					raise MySQLdb.InterfaceError(0, 'No connect for cursor [first connect]')

				# create new cursor
				self.__create()

				# test for execute
				self.__cursor.execute('SELECT now()')

			return True


		def execute(self, query='', param=()):
			""" cursor execute """

			# default
			found = 0

			# test if cursor exist?
			self.__test()

			# sql execute
			start_time = time.time()

			__query = query % tuple(param)
			log.debug('mysql %s execute: "%s"', (self.__name, __query), priority=4)

			try:
				found = self.__cursor.execute(query, param)

			# mysql error
			except MySQLdb.OperationalError as mysql_err:
				log.error('mysql %s execute: OperationalError="%s"',\
					(self.__name, mysql_err), priority=2)

				# reset cursor + try reconnect
				self.__cursor = None
				self.__test()

				found = self.__cursor.execute(query, param)

			# unspecified error
			except BaseException as base_err:
				log.error('mysql %s execute: err="%s"',\
					(self.__name, base_err), priority=2)
				raise

			run_time = int((time.time() - start_time) * 1000)

			log.info('mysql %s execute query: "%s" time=%sms found=%s dbh.thread_id=%s',\
				(self.__name, __query, run_time, found, self.__thread_id), priority=2)

			return found


		def __fetch(self, fetch_type='all'):
			""" fetch """

			start_time = time.time()

			if fetch_type == 'one':
				ret = self.__cursor.fetchone()
			else:
				ret = self.__cursor.fetchall()

			run_time = int((time.time() - start_time) * 1000)

			log.info('mysql %s fetch%s query: time=%sms found=%s',\
				(self.__name, fetch_type, run_time, len(ret)), priority=2)

			return ret


		def fetchall(self):
			""" cursor fetchall """

			return self.__fetch('all')


		def fetchone(self):
			""" cursor fetchone """

			return self.__fetch('one')


		def insert_id(self):
			""" last insert_id """

			try:
				insert_id = self.__dbh.insert_id()
			except BaseException as base_err:
				log.error('mysql %s insert_id(): err=%s',\
					(self.__name, base_err), priority=2)
				return 0

			log.info('mysql %s insert_id(): %s',\
				(self.__name, insert_id), priority=2)

			return insert_id


		def close(self):
			""" cursor close """

			log.debug('cursor.close()')

			try:
				self.__cursor.close()

			except BaseException as emsg:
				log.error('mysql %s cursor: close err="%s"',\
					(self.__name, emsg), priority=2)


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
				log.error('select: table_name must be input', priority=2)
				return 0, None

			(sql_where, sql_param) = _sql_where(where_dict)

			found = self.execute('SELECT %s FROM `%s` %s %s' % (\
				_sql_column(column_list, table_name), table_name,\
				sql_where, _sql_limit(limit)),\
				sql_param)

			if found == 0:
				return found, []

			return found, self.__cursor.fetchall()

		def insert(self, table_name, value_dict):
			""" Simple INSERT """
			return self.__insert('INSERT', table_name, value_dict)

		def replace(self, table_name, value_dict):
			""" Simple REPLACE """
			return self.__insert('REPLACE', table_name, value_dict)

		def __insert(self, sql_type, table_name, value_dict):
			""" Simple INSERT / REPLACE """

			if not table_name or not value_dict:
				log.error('insert: table_name/value_dict must be input', priority=2)
				return 0

			(sql_set, sql_param) = _sql_set(value_dict)

			found = self.execute('%s INTO `%s` SET %s' % (\
				sql_type, table_name, sql_set),\
				sql_param)

			return found


		def update(self, table_name, value_dict, where_dict, limit=0):
			""" Simple UPDATE """

			if not table_name and not value_dict:
				log.error('update: table_name/value_dict must be input', priority=2)
				return 0

			(sql_set, sql_param) = _sql_set(value_dict)
			(sql_where, params2) = _sql_where(where_dict)

			for param_name in params2:
				sql_param.append(param_name)

			found = self.execute('UPDATE `%s` SET %s %s %s' % (\
				table_name, sql_set, sql_where, _sql_limit(limit)),\
				sql_param)

			return found


		def delete(self, table_name, where_dict=None, limit=0):
			""" Simple DELETE """

			if not table_name:
				log.error('delete: table_name must be input', priority=2)
				return 0

			if not where_dict:
				where_dict = {}

			(sql_where, sql_param) = _sql_where(where_dict)

			found = self.execute('DELETE FROM `%s` %s %s' % (\
				table_name, sql_where, _sql_limit(limit)),\
				sql_param)

			return found

		def now(self):
			""" now """

			return _sql_now()
