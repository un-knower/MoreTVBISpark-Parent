package com.moretv.bi.util

import java.sql.{Connection, DriverManager, SQLException}
import java.util.{List, Map}

import com.moretv.bi.constant.{Constants, Database}
import org.apache.commons.dbutils.{DbUtils, QueryRunner}
import org.apache.commons.dbutils.handlers.{ArrayHandler, ArrayListHandler, MapListHandler}
import org.apache.commons.lang.StringUtils

/**
 * Created by Will on 2015/3/3.
 */
class DBOperationUtils(var url:String,user:String,password:String,database:String){
  private val driver: String = "com.mysql.jdbc.Driver"
  private var conn: Connection = null
  private var queryRunner: QueryRunner = null


  try {
    Class.forName(driver)
    if (StringUtils.isNotBlank(database)) {
      url = url.replace("@#@", database)
    }
    //else throw new RuntimeException("The database name(" + database + ") is not valid!")
    conn = DriverManager.getConnection(url, user, password)
    queryRunner = new QueryRunner
  }
  catch {
    case e: Exception => {
      e.printStackTrace
    }
  }

  def this(url:String,user:String,password:String) =
    this(url,user,password,null)

  def this(database:String)=
    this(s"jdbc:mysql://${Constants.HZ_15_HOST}:${Constants.HZ_MYSQL_PORT}/@#@?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",Database.DB_2_15_USER,Database.DB_2_15_PASSWORD,database)





  /**
   * 向数据库中插入记录
   * @param sql 预编译的sql语句
   * @param params 插入的参数
   * @return 影响的行数
   * @throws SQLException
   */
  def insert(sql: String, params: Object*): Int = {
    return queryRunner.update(conn, sql, params: _*)
  }

  /**
   * Update the data of the database
   */
  def update(sql:String,params: Object*): Int ={
    return queryRunner.update(conn,sql,params:_*)
  }

  /**
   * 通过指定的SQL语句和参数查询数据
   * @param sql 预编译的sql语句
   * @param params 查询参数
   * @return 查询结果
   */
  def selectOne(sql: String, params: Object*):Array[AnyRef] = {
    return queryRunner.query(conn, sql, new ArrayHandler(), params: _*)
  }

  /**
   * 通过指定的SQL语句和参数查询数据
   * @param sql 预编译的sql语句
   * @param params 查询参数
   * @return 查询结果
   */
  def selectMapList(sql: String, params: Object*): List[Map[String,Object]] = {
    return queryRunner.query(conn, sql, new MapListHandler(), params: _*)
  }

  /**
   * 通过指定的SQL语句和参数查询数据
   * @param sql 预编译的sql语句
   * @param params 查询参数
   * @return 查询结果
   */
  def selectArrayList(sql: String, params: Object*): List[Array[Object]] = {
    return queryRunner.query(conn, sql, new ArrayListHandler(), params: _*)
  }

  /**
   * 删除错乱的数据
   * @param sql delete sql
   * @param params delete sql params
   * @return
   */
  def delete(sql: String,params: Object*) = {
    queryRunner.update(conn, sql,params: _*)
  }

  /**
   * 释放资源，如关闭数据库连接
   */
  def destory() {
    DbUtils.closeQuietly(conn)
  }
}

