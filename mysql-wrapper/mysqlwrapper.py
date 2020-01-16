#!/usr/bin/python3
# -*- coding: utf-8 -*-

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
	""" mysql connect object """

	def __init__(self, host='localhost', db='', user='root', passwd='', port=3306,\
		charset='utf8', autocommit=1, conv=None, connect_timeout=5, dict_cursor=1, dummy=1):
		""" init """

		# input params
		self.__host = host
		self.__db = db
		self.__user = user
		self.__passwd = passwd
		self.__port = port
		self.__connect_timeout = connect_timeout
		self.__dict_cursor = dict_cursor

		self.__charset = charset
		self.__autocommit = autocommit
		self.__conv = conv
		self.__dummy = dummy

		# default
		self.__dbh = None

		#if not self.__conv:
		#	from MySQLdb.converters import conversions
		#	conversions = conversions.copy()
		#	conversions[Fixed] = conv_fixed
		#	conversions[types.ListType]  = conv_array
		#	self.__conv = conversions

		# connect
		self.__connect_time = 0
		self.__connect()


	def __connect(self):
		""" connect """

		start_time = time.time()

		# connect
		try:
			self.__dbh = MySQLdb.Connect(\
				host=self.__host,\
				db=self.__db,\
				user=self.__user,\
				passwd=self.__passwd,\
				port=self.__port,\
				connect_timeout=self.__connect_timeout)
			self.__connect_time = time.time() - start_time

		except Exception as emsg:
			print('connect error=%s' % emsg)
			if self.__dummy:
				print('mysql no connect')
				return -1

			raise

		# set autocommit an charset
		self.__dbh.autocommit(self.__autocommit)
		self.__dbh.set_character_set(self.__charset)

		# connect time
		self.__connect_time = time.time() - start_time


	def cursor(self, dict_cursor=None):
		""" mysql cursor """

		if not dict_cursor:
			return self.Cursor(dbh=self.__dbh, dict_cursor=self.__dict_cursor)

		return self.Cursor(dbh=self.__dbh, dict_cursor=dict_cursor)


	class Cursor(object):
		""" mysql cursor object """

		def __init__(self, dbh, dict_cursor=None):
			""" init """

			# input params
			self.__dbh = dbh
			self.__dict_cursor = dict_cursor

			# default
			self.__cursor = None

			# create cursor
			self.__create()


		#def __getattr__(self, name):
		#	return getattr(self.__cursor, name)


		def __create(self):
			""" create cursor """

			if not self.__dbh:
				print('mysql cursor: no connect')
				return False

			if self.__dict_cursor:
				self.__cursor = self.__dbh.cursor(MySQLdb.cursors.DictCursor)
				return True

			self.__cursor = self.__dbh.cursor()
			return True


		def execute(self, query='', param=()):
			""" cursor execute """

			# default
			found = 0

			# reconnect id cursor not exist
			if not self.__cursor:
				ret_connect = Connect.__connect(self)
				if ret_connect == -1:
					raise MySQLdb.InterfaceError('No connect for cursor [first connect]')

				self.__cursor()

			# sql execute
			start_time = time.time()

			try:
				found = self.__cursor.execute(query, param)

			except MySQLdb.OperationalError as mysql_error:
				ret_connect = Connect.__connect(self)
				if ret_connect == -1:
					raise MySQLdb.InterfaceError('No connect for cursor, err="%s"' % mysql_error)

				found = self.__cursor.execute(query, param)

			except Exception as emsg:
				print('mysql Exception, err="%s"', emsg)
				raise

			run_time = time.time() - start_time

			print('execute query time=%.3fs found=%s' % (run_time, found))

			return found


		def fetchall(self):
			""" cursor fetchall """

			start_time = time.time()
			ret = self.__cursor.fetchall()
			run_time = time.time() - start_time

			print('fetchall query time=%.3fs' % (run_time,))

			return ret


		def fetchone(self):
			""" cursor fetchone """

			start_time = time.time()
			ret = self.__cursor.fetchone()
			run_time = time.time() - start_time

			print('fetchone query time=%.3fs' % (run_time,))

			return ret



if __name__ == '__main__':

	import pprint

	DBH = Connect(user='test_user', passwd='test_passwd', db='test_db')
	CURSOR = DBH.cursor()
	CURSOR.execute('SELECT * FROM test_table LIMIT 5')
	pprint.pprint(CURSOR.fetchall())
