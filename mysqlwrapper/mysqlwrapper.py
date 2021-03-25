#!/usr/bin/python3
# -*- coding: utf-8 -*-

# todo:
# pylint: disable=no-member
# pylint: disable=protected-access
# pylint: disable=no-self-use

"""
MySQL wrapper
"""

import time
import random

from mysqlwrapper_util import _sql_where, _sql_column, _sql_limit, _sql_set, _sql_now
from logni import log


try:
	#import MySQLdb
	import pymysql
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
		self.__param = {\
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
			'separate connect':param.get('separate connect', 0),\
			'name':'',\
		}

		# connect
		self.__thread_id = 0
		self.__connect_time = 0
		self.__dbc = None
		self.connect()


	def connect(self):
		""" connect """
		start_time = time.time()

		# name
		if not self.__param['name']:
			self.__param['name'] = '%s.%s' % \
				(self.__param['host'], self.__param['db'])
		self.__name = self.__param['name']

		# dbh
		self.__dbc = None

		# connect
		try:

			dbc = pymysql.connect(host=self.__param['host'],\
				user=self.__param['user'],\
				password=self.__param['passwd'],\
				db=self.__param['db'],\
				port=self.__param['port'],\
				charset=self.__param['charset'],\
				cursorclass=pymysql.cursors.DictCursor)

			self.__connect_time = time.time() - start_time

		except BaseException as emsg:
			log.error('mysql INIT host=%s:%s, user=%s, connect error=%s',\
				(self.__param['host'], self.__param['port'], self.__param['user'],\
				emsg), priority=2)
			if self.__param.get('dummy', 1):
				log.warn('mysql INIT host=%s:%s, user=%s, no connect',\
					(self.__param['host'], self.__param['port'],\
					self.__param['user']), priority=2)
				return False

			raise

		# set autocommit an charset
		dbc.autocommit(self.__param['autocommit'])

		self.__dbc = dbc

		# connect time
		self.__connect_time = int((time.time() - start_time) * 1000)

		log.info('mysql INIT host=%s:%s, user=%s -> name=%s, thread_id=%s, time=%sms, connect OK',\
			(self.__param['host'], self.__param['port'], self.__name,\
			self.__param['user'], self.thread_id(), self.__connect_time), priority=2)

		# set wait_timeout
		cursor = dbc.cursor()
		cursor.execute('SET wait_timeout = 3600')
		cursor.close()

		return dbc


	def __dbi(self):
		""" connect db """

		if self.__param['separate connect'] == 1:
			return self.connect()

		return self.__dbc


	def __db_close(self, dbh):
		""" close db """

		if self.__param['separate connect'] == 1:
			log.debug('mysql %s close -> thread_id=%s', (self.__name, dbh.thread_id), priority=2)
			dbh.close()
			return True

		return True



	def thread_id(self):
		""" thread id """

		self.__thread_id = self.__dbc.thread_id()
		return self.__thread_id


	def commit(self):
		""" commit """

		try:
			self.__dbc.commit()
		except BaseException as emsg:
			log.error('mysql %s commit: err="%s"',\
				(self.__name, emsg), priority=2)
			return False

		log.info('mysql %s commit', self.__name, priority=2)
		return True


	def close(self):
		""" close """

		thread_id = self.__dbc.thread_id()

		try:
			self.__dbc.close()
		except BaseException as emsg:
			log.error('mysql %s close: thread_id=%s, err="%s"',\
				(self.__name, thread_id, emsg), priority=2)
			return False

		log.info('mysql %s close: thread_id=%s', (self.__name, thread_id), priority=2)
		return True


	def __execute(self, cursor, query='', param=(), rand=0):
		""" cursor execute """

		# default
		found = 0

		# test if cursor exist?
		#self.__test()

		# sql execute
		start_time = time.time()

		__query = query % tuple(param)
		#log.debug('mysql %s execute [%s]: "%s"', (self.__name, rand, __query), priority=4)

		#try:
		found = cursor.execute(query, param)

		# mysql error
		#except MySQLdb.OperationalError as mysql_err:
		#	log.error('mysql %s execute [%s]: OperationalError="%s"',\
		#		(self.__name, rand, mysql_err), priority=2)
		#
		#	# reset cursor + try reconnect
		#	cursor.close()
		#	cursor = self.__dbc.cursor()
		#
		#	found = cursor.execute(query, param)
		#
		# unspecified error
		#except BaseException as base_err:
		#	log.error('mysql %s execute [%s]: err="%s"',\
		#		(self.__name, rand, base_err), priority=2)
		#	raise

		run_time = int((time.time() - start_time) * 1000)

		log.info('mysql %s execute [%s] query: "%s" time=%sms found=%s dbc.thread_id=%s',\
			(self.__name, rand, __query, run_time, found, self.__thread_id), priority=2)

		return cursor, found


	def __fetchall(self, cursor, rand=0):
		""" fetch """

		start_time = time.time()

		data = cursor.fetchall()

		run_time = int((time.time() - start_time) * 1000)

		log.info('mysql %s fetchall [%s] query: time=%sms found=%s',\
			(self.__name, rand, run_time, len(data)), priority=2)

		return cursor, data


	def query(self, query, param=()):
		""" sql query """

		rand = random.randint(1000, 9999)
		dbh = self.__dbi()
		cursor = dbh.cursor()
		cursor, found = self.__execute(cursor, query, param, rand)

		if found == 0:
			cursor.close()
			self.__db_close(dbh)
			del rand, cursor, dbh
			return found, []

		cursor, data = self.__fetchall(cursor, rand)
		cursor.close()
		self.__db_close(dbh)

		del rand, cursor, dbh
		return found, data




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

		rand = random.randint(1000, 9999)

		dbh = self.__dbi()
		cursor = dbh.cursor()
		cursor, found = self.__execute(cursor, 'SELECT %s FROM `%s` %s %s' % (\
			_sql_column(column_list, table_name), table_name,\
			sql_where, _sql_limit(limit)),\
			sql_param, rand)

		if found == 0:
			#self.__cursor.nextset()
			#log.debug('mysql %s nextset -> found=0', self.__name)
			cursor.close()
			self.__db_close(dbh)
			del rand, cursor, dbh
			return found, []

		cursor, data = self.__fetchall(cursor, rand)
		cursor.close()
		self.__db_close(dbh)

		del rand, cursor, dbh
		return found, data


	def insert_id(self, table_name, value_dict):
		""" Simple INSERT """
		return self.__insert('INSERT', table_name, value_dict, 1)

	def insert(self, table_name, value_dict):
		""" Simple INSERT """
		return self.__insert('INSERT', table_name, value_dict, 0)

	def replace(self, table_name, value_dict):
		""" Simple REPLACE """
		return self.__insert('REPLACE', table_name, value_dict, 0)

	def __insert(self, sql_type, table_name, value_dict, insertid=0):
		""" Simple INSERT / REPLACE """

		if not table_name or not value_dict:
			log.error('insert: table_name/value_dict must be input', priority=2)
			return 0

		(sql_set, sql_param) = _sql_set(value_dict, sql_type)

		dbh = self.__dbi()
		cursor = dbh.cursor()
		cursor, found = self.__execute(cursor, '%s INTO `%s` SET %s' % (\
			sql_type, table_name, sql_set),\
			sql_param)
		cursor.close()

		# insert id
		if insertid:
			found = dbh.insert_id()

		self.__db_close(dbh)

		del cursor
		return found


	def update(self, table_name, value_dict, where_dict, limit=0):
		""" Simple UPDATE """

		if not table_name and not value_dict:
			log.error('update: table_name/value_dict must be input', priority=2)
			return 0

		(sql_set, sql_param) = _sql_set(value_dict, 'update')
		(sql_where, params2) = _sql_where(where_dict)

		for param_name in params2:
			sql_param.append(param_name)

		dbh = self.__dbi()
		cursor = dbh.cursor()
		cursor, found = self.__execute(cursor, 'UPDATE `%s` SET %s %s %s' % (\
			table_name, sql_set, sql_where, _sql_limit(limit)),\
			sql_param)
		cursor.close()
		self.__db_close(dbh)

		del cursor
		return found


	def delete(self, table_name, where_dict=None, limit=0):
		""" Simple DELETE """

		if not table_name:
			log.error('delete: table_name must be input', priority=2)
			return 0

		if not where_dict:
			where_dict = {}

		(sql_where, sql_param) = _sql_where(where_dict)

		dbh = self.__dbi()
		cursor = dbh.cursor()
		cursor, found = self.__execute(cursor, 'DELETE FROM `%s` %s %s' % (\
			table_name, sql_where, _sql_limit(limit)),\
			sql_param)
		cursor.close()
		self.__db_close(dbh)

		del cursor
		return found


	def now(self):
		""" now """

		return _sql_now()
