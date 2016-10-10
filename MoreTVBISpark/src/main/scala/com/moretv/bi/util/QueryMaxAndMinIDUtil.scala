package com.moretv.bi.util

import java.sql.DriverManager

/**
 * Created by laishun on 15/9/5.
 */
trait QueryMaxAndMinIDUtil {
  def queryID(colume:String, tableName:String , url:String):Array[Long]={
    val driver = "com.mysql.jdbc.Driver"
    val username = "bi"
    val password = "mlw321@moretv"
    Class.forName(driver)
    val connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement()
    val  sql = "select Max("+colume+"),Min("+colume+") from "+ tableName;
    val res = statement.executeQuery(sql);
    res.next();
    val id = new Array[Long](2);
    id(0)=res.getInt(1).toLong;
    id(1)=res.getInt(2).toLong;
    id
  }
}
