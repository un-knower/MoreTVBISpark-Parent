#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb
import warnings
import ConfigParser
import sys
import os

'''
method 1:get tables name from information_schema

'''


class MysqlBase():
    # 初始化mysql连接
    def __init__(self, ip="10.10.1.1", user="root", passwd='11', databases='zabbix'):
        self.ip = ip
        self.user = user
        self.passwd = passwd
        self.databases = databases
        try:
            self.conn = MySQLdb.connect(self.ip, self.user, self.passwd, self.databases)
        except:
            raise ValueError("mysql connect failed %s") % self.ip
        self.curs = self.conn.cursor()
        self.curs.execute('SET NAMES utf8')

    def __del__(self):
        self.curs.close()
        self.conn.close()

    def insert(self, table, data):
        _field = ','.join(['`%s`' % (k_insert) for k_insert in data.keys()])
        _value = ','.join(["'%s'" % (self.conn.escape_string(str(v_insert))) for v_insert in data.values()])
        _sql = 'INSERT INTO `%s`(%s) VALUES(%s)' % (table, _field, _value)
        print _sql

        self.curs.lastrowid = 0
        try:
            self.curs.execute(_sql)
            self.conn.commit()
        except:
            self.conn.rollback()
            raise ValueError("insert error %s" % _sql)

        return self.curs.lastrowid

    def update(self, table, data, condition):
        _field = ','.join(["`%s`='%s'" % (k_update, self.conn.escape_string(str(data[k_update]))) for k_update in data])
        _sql = "UPDATE `%s` SET %s where %s" % (table, _field, condition)

        resNum = 0
        try:
            resNum = self.curs.execute(_sql)
            self.conn.commit()
        except:
            self.conn.rollback()
            raise ValueError("update error %s" % _sql)

        return resNum

    def delete(self, table, condition):
        _sql = 'DELETE FROM `%s` WHERE %s' % (table, condition)
        resNum = 0
        try:
            resNum = self.curs.execute(_sql)
            self.conn.commit()
        except:
            self.conn.rollback()
            raise ValueError('delete error %s' % _sql)
        return resNum

    def query(self, sql):
        res = ''
        try:
            self.curs.execute(sql)
            res = self.curs.fetchall()
        except:
            raise ValueError('query error %s' % sql)
        return res

    def change(self, sql, many=False):
        ###过滤unknow table的warning
        warnings.filterwarnings('ignore')
        resNum = 0
        if many:
            try:
                resNum = self.curs.executemany(sql, many)
                self.conn.commit()
            except:
                self.conn.rollback()
                raise ValueError('exec error %s' % sql)
        else:
            try:
                resNum = self.curs.execute(sql)
                self.conn.commit()
            except:
                self.conn.rollback()
                raise ValueError('exec error %s' % sql)

        return resNum


def initlog():
    import logging
    logger = None
    logger = logging.getLogger()
    log_name = sys.argv[0].split(".")[0] + ".log"
    hdlr = logging.FileHandler(log_name)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.NOTSET)
    return [logger, hdlr]


def logMsg(fun_name, err_msg, level):
    message = fun_name + ':' + err_msg
    logger, hdlr = initlog()
    logger.log(level, message)
    hdlr.flush()
    logger.removeHandler(hdlr)  # 网络上的实现基本为说明这条语句的使用和作用


def get_config(filename, values):
    cf = ConfigParser.SafeConfigParser()
    cf.read(filename)
    configDataSection = cf.sections()
    returnData = {}

    if values in configDataSection:
        _list = cf.items(values)
        for _key, _value in _list:
            returnData[_key] = _value
    else:
        print "[ERROR] %s is not in config files,PLS check it %s" % (values, filename)
        msg_info = "===%s: Get info Failed!!===" % values
        logMsg("get_config", msg_info, 2)
        raise "Values could`t found in config"

    return returnData


def get_table_row(data, day):
    hostip = data.get('hostip', None)
    user = data.get('user', None)
    passwd = data.get('passwd', None)
    db = data.get('db', None)
    all_table = list()
    check_result = dict()
    # ip="10.10.1.1", user="root", passwd='11', databases='zabbix'
    m = MysqlBase(ip=hostip, user=user, passwd=passwd, databases=db)

    get_table_sql = "select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA='%s' " % db
    tables = m.query(get_table_sql)
    for tables_line in tables:
        all_table.append(tables_line[0])

    for check_table in all_table:
        count_sql = "select count(*) from %s where day = '%s'" % (check_table, day)
        try:
            check_data = m.query(count_sql)
            print "Get values from table %s" % check_table
            if check_data:
                check_key = "%s%s" % (check_table, day)
                check_result[check_key] = check_data[0][0]
        except:
            msg = "get values from table %s error,maybe no row" % check_table
            logMsg("get_values", msg, 2)
            pass

    print "check values complete"
    return check_result


def write_result(filename, data):
    with open(filename, 'w') as f:
        f.write(data)

    return True


def main():
    filename = sys.argv[1]
    output = "diff_row.log"
    day = sys.argv[2]

    source_config = get_config(filename, "source")
    dest_config = get_config(filename, "dest")

    source_data = get_table_row(source_config, day)
    dest_data = get_table_row(dest_config, day)

    for key in source_config.keys():
        if source_data.get(key, None) != dest_data.get(key, None):
            msg = "tables %s ,source line %s ,dest line %s \n" % (
                key, source_data.get(key, None), dest_data.get(key, None))
            write_result(output, msg)
            logMsg("diff", msg, 2)

    print "END"


if __name__ == '__main__':
    main()
