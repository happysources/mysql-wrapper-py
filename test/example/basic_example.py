#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Example for MySQL wrapper
"""

import sys
import pprint
sys.path.append('mysql-wrapper')

import mysqlwrapper

DBH = mysqlwrapper.Connect(user='test_user', passwd='t3st_p8sSwd', db='test_db')
CURSOR = DBH.cursor()
CURSOR.execute('SELECT * FROM test_table LIMIT 5')
pprint.pprint(CURSOR.fetchall())
CURSOR.close()
DBH.close()
