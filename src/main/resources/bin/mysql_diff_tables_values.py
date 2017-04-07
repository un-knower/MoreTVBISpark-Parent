#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb
import warnings
import ConfigParser
import sys
import os
import pdb
import json

'''
method 2:get values from source ,check it in dest

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
            msg = 'query error %s' % sql
            logMsg("sql_query", msg, 2)
            res = None
            # raise ValueError('query error %s' % sql)
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


def get_tables_values(data, day):
    hostip = data.get('hostip', None)
    user = data.get('user', None)
    passwd = data.get('passwd', None)
    db = data.get('db', None)
    tables = data.get('tables', None).split(",")
    m = MysqlBase(ip=hostip, user=user, passwd=passwd, databases=db)
    return_data = dict()
    for one_table in tables:
        sql = "select * from %s where day='%s'" % (one_table, day)
        values = m.query(sql)
        return_data[one_table] = values
    return return_data


def find_diff_values(s_dict, d_dict, s_hostip):
    s_keys = s_dict.keys()
    d_keys = d_dict.keys()

    for s_key in s_keys:
        if s_key not in d_keys:
            break
        else:
            for s_line in s_dict[s_key]:
                if not s_line:
                    msg = "table %s no 'day' keys" % s_key
                    logMsg("diff", msg, 2)
                    break
                # 排除ID字段
                check_values = s_line[1:]
                is_same = False
                for dest_line in d_dict[s_key]:
                    if check_values == dest_line[1:]:
                        is_same = True
                        break
                if not is_same:
                    message = "host %s tables %s ,values not find :%s" % (s_hostip,
                                                                          s_key,
                                                                          json.dumps(check_values, indent=1).replace(
                                                                                  "\n", "\t"))
                    logMsg("check_values", message, 2)


def main():
    filename = sys.argv[1]
    day = sys.argv[2]
    source_config = get_config(filename, "source")
    dest_config = get_config(filename, "dest")

    source_data = get_tables_values(source_config, day)
    dest_data = get_tables_values(dest_config, day)

    diff_set = list(set(source_data.keys()) ^ set(dest_data.keys()))
    if diff_set:
        msg_table = "find some talbe diff ,named %s" % json.dumps(diff_set, indent=1)
        logMsg("table_diff", msg_table, 2)

    find_diff_values(source_data, dest_data, source_config['hostip'])
    find_diff_values(dest_data, source_data, dest_config['hostip'])

    print "check END"


if __name__ == '__main__':
    main()
