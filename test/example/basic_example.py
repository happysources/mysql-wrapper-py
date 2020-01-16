#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Example for MySQL wrapper
"""

import sys
import pprint
sys.path.append('mysql-wrapper')

import mysqlwrapper

DBH = mysqlwrapper.Connect(user='test_user', passwd='test_passwd', db='test_db')
CURSOR = DBH.cursor()
CURSOR.execute('SELECT * FROM test_table LIMIT 5')
pprint.pprint(CURSOR.fetchall())
