package com.moretv.bi.util

import java.sql.DriverManager

/**
 * Created by mycomputer on 2015/3/27.
 */
trait MysqlSetting {
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://10.10.2.15:3306/bi?useUnicode=true&characterEncoding=utf-8&autoReconnect=true"
  val username = "bi"
  val password = "mlw321@moretv"
  Class.forName(driver)
  val connection = DriverManager.getConnection(url, username, password)
  var statement = connection.createStatement()
}
