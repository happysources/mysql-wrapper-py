#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Example for MySQL wrapper
"""

import sys
import pprint
import time
sys.path.append('mysqlwrapper')

import mysqlwrapper

SLEEP = 0
DBH = mysqlwrapper.Connect(user='test_user', passwd='test_passwd', db='test_db', param={'debug':1})
print(dir(DBH))

pprint.pprint(DBH.query('SELECT * FROM test_table LIMIT 5'))
time.sleep(SLEEP)
print()

pprint.pprint(DBH.query('SELECT * FROM test_table LIMIT 5'))
time.sleep(SLEEP)
print()

print('simple select without where')
print(DBH.select(table_name='test_table', where_dict=None, column_list=[], limit=0))
print()
time.sleep(SLEEP)

print('simple select with where id=1')
print(DBH.select(table_name='test_table', limit=0, where_dict={'id':1}))
print()

print('simple select - not found')
print(DBH.select(table_name='test_table', limit=0, where_dict={'id':1000}))
print()

print('simple update')
print(DBH.update('test_table', {'value_str':'one 1'}, {'id':1}, limit=1))
print()

print('simple insert')
print(DBH.insert('test_table', {'value_str':'new', 'value_int':100}))
print()

print('simple delete')
print(DBH.delete('test_table', {'value_str':'new', 'value_int':100}, limit=1))
DBH.commit()
print()

print('reconnect:')
DBH.close()
DBH.connect()
print()

print('cursor execute: select')
for ni in range(1, 3):
	print(DBH.query('SELECT * FROM test_table LIMIT 1'))
	print(DBH.query('SELECT * FROM test_table LIMIT 2'))
print()

DBH = mysqlwrapper.Connect(user='test_user', passwd='test_passwd', db='test_db', param={'debug':1, 'separate_connect':1})
print('cursor execute: insert')
for no in range(100, 104):
	pprint.pprint(DBH.insert('test_table', {'value_str':'new%s' %no, 'value_int':no}))

DBH.close()
