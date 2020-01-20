#!/usr/bin/python3
# -*- coding: utf-8 -*-

# pylint: disable=no-member

"""
MySQL wrapper
"""

import sys
import time

try:
	import MySQLdb
except ImportError as import_err:
	print('run $ pip install -r requirements.txt, please [err="%s"]' % import_err)
	sys.exit(1)


class Connect(object):
	""" MySQLwrapper Connect object """

	def __init__(self, user='root', passwd='', db='', host='localhost', param={}, **kw):
		"""
		Connect initialize

		Parameters:
			user (str): mysql user
			passwd (str): mysql password
			db (str): mysql database
			host (str): mysql hostname or ip
			param (struct): mysql parameters for connect

		Returns:
			None
		"""

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


	def __getattr__(self,name):
        	return getattr(self.__share['dbh'], name)


	def _connect(self):
		""" connect """

		start_time = time.time()

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
			print('connect error=%s' % emsg)
			if self._param.get('dummy',1):
				print('mysql no connect')
				return -1

			raise

		# set autocommit an charset
		dbh.autocommit(self._param['autocommit'])
		dbh.set_character_set(self._param['charset'])

		# connect time
		self.__connect_time = time.time() - start_time

		self.__share['dbh'] = dbh


	def _dbh(self):
		""" dbh """
		return self.__share['dbh']


	def cursor(self, dict_cursor=None):
		""" mysql cursor """

		if not dict_cursor:
			dict_cursor=self._param['dict_cursor']

		return self.Cursor(dbh=self, dict_cursor=dict_cursor, debug=self._param['debug'])


	def close(self):
		""" close """

		try:
			self.__share['dbh'].close()
		except BaseException as emsg:
			print('mysql Exception, database close err="%s"', emsg)



	class Cursor(object):
		""" MySQLwrapper Cursor object """

		def __init__(self, dbh=None, dict_cursor=None, debug=0):
			"""
			Cursor initialize

			Parameters:
				dbh (object): database connect
				dict_cursor (int): dictionary cursor

			Returns:
				None
			"""

			# input params
			self.__dbh = dbh
			self.__dict_cursor = dict_cursor
			self.__debug = debug

			# default
			self.__cursor = None

			# create cursor
			self.__create()


		def __getattr__(self, name):
			return getattr(self.__cursor, name)


		def __create(self):
			""" create cursor """

			if self.__debug:
				print('create() __dbh:',dir(self.__dbh))
				print(self.__dbh.__class__)

			if not self.__dbh._dbh():
				print('mysql cursor: no connect')
				return False

			if self.__dict_cursor:
				self.__cursor = self.__dbh._dbh().cursor(MySQLdb.cursors.DictCursor)
				return True

			self.__cursor = self.__dbh._dbh().cursor()
			return True


		def __test(self):
			""" tested cursor if exist? """

			if self.__debug:
				print('test() __dbh:',dir(self.__dbh))
				print(self.__dbh.__doc__)

			# reconnect id cursor not exist
			if not self.__cursor:
				ret_connect = self.__dbh._connect()
				if ret_connect == -1:
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

			try:
				found = self.__cursor.execute(query, param)

			except MySQLdb.OperationalError as mysql_error:
				print('MySQLdb.OperationalError, err="%s"' % mysql_error)
				ret_connect = Connect._connect(self)
				if ret_connect == -1:
					raise MySQLdb.InterfaceError(0, 'No connect for cursor, err="%s"' % mysql_error)

				found = self.__cursor.execute(query, param)

			except BaseException as emsg:
				print('mysql Exception, execute err="%s"', emsg)
				raise

			run_time = time.time() - start_time

			print('execute query time=%.3fs found=%s' % (run_time, found))

			return found


		def __fetch(self, fetch_type='all'):
			""" fetch """

			start_time = time.time()

			if fetch_type == 'one':
				ret = self.__cursor.fetchone()
			else:
				ret = self.__cursor.fetchall()

			run_time = time.time() - start_time

			print('fetch%s query time=%.3fs' % (fetch_type, run_time,))

			return ret


		def fetchall(self):
			""" cursor fetchall """
			return self.__fetch('all')


		def fetchone(self):
			""" cursor fetchone """
			return self.__fetch('one')


		def close(self):
			""" cursor close """

			try:
				self.__cursor.close()

			except Exception as emsg:
				print('mysql Exception, cursor close err="%s"', emsg)
