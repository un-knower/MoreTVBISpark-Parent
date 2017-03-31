#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb

# 打开数据库连接
db = MySQLdb.connect("bigdata-extsvr-db_bi1","bi","mlw321@moretv","medusa" )
 # 使用cursor()方法获取操作游标
cursor = db.cursor()

# 使用execute方法执行SQL语句
cursor.execute("SELECT VERSION()")
# 使用 fetchone() 方法获取一条数据库。
data = cursor.fetchone()
print "Database version : %s " % data


# SQL 查询语句
sql = "select day,channelname,tabname,play_user,play_num from medusa_channel_eachtab_play_xiqu_info where day='2017-03-21' order by tabname  "

try:
   # 执行SQL语句
   cursor.execute(sql)
   # 获取所有记录列表
   results = cursor.fetchall()
   for row in results:
      day = row[0]
      channelname = row[1]
      tabname = row[2]
      play_user = row[3]
      play_num = row[4]
      # 打印结果
      print "day=%s,channelname=%s,tabname=%s,play_user=%d,play_num=%d" % \
             (day, channelname, tabname, play_user, play_num )
except:
   print "Error: unable to fecth data"


# 关闭数据库连接
db.close()