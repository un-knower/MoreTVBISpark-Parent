package com.moretv.bi.util

import java.sql.DriverManager

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases

/**
 * Created by mycomputer on 2015/3/27.
 */
@deprecated
trait MysqlSetting {
  val driver = "com.mysql.jdbc.Driver"
  val db = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
  val url = db.prop.getProperty("url")
  val username = db.prop.getProperty("user")
  val password = db.prop.getProperty("password")
  Class.forName(driver)
  val connection = DriverManager.getConnection(url, username, password)
  var statement = connection.createStatement()
}
