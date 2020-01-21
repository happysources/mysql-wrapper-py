#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Example for MySQL wrapper
"""

import sys
import pprint
sys.path.append('mysqlwrapper')

import mysqlwrapper

DBH = mysqlwrapper.Connect(user='test_user', passwd='test_passwd', db='test_db', param={'debug':1})
CURSOR = DBH.cursor()

CURSOR.execute('SELECT * FROM test_table LIMIT 5')
pprint.pprint(CURSOR.fetchall())
print()

CURSOR.execute('SELECT * FROM test_table LIMIT 5')
pprint.pprint(CURSOR.fetchone())
print()

print('simple select without where')
print(CURSOR.select(table_name='test_table', where_dict=None, column_list=[], limit=0))
print()

print('simple select with where id=1')
print(CURSOR.select(table_name='test_table', limit=0, where_dict={'id':1}))
print()

print('simple update')
print(CURSOR.update('test_table', {'value_str':'one 1'}, {'id':1}, limit=1))
print()

print('simple insert')
print(CURSOR.insert('test_table', {'value_str':'new', 'value_int':100}))
print()

print('simple delete')
print(CURSOR.delete('test_table', {'value_str':'new', 'value_int':100}, limit=1))
print()

CURSOR.close()
DBH.close()
