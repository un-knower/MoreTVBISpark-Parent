package com.moretv.bi.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases

import scala.collection.mutable.Map

/**
  * Created by xiajun on 2016/8/2.
  * 用于获取账户的相关信息
  */
@deprecated
object AccountIDInfoUtils {
  /**
    * 定义一些map集合
    */
  var accountGenderMap = Map[String, String]()
  var accountAgeMap = Map[String, String]()

  /**
    * 定义一些常量
    */
  val driver: String = "com.mysql.jdbc.Driver"
  val db1 = DataIO.getMySqlOps(DataBases.MORETV_RECOMMEND_TVSERVICE_MYSQL)
  val  user:String = db1.prop.getProperty("user")
  val password:String = db1.prop.getProperty("password")


  val url_ucenter_17: String = db1.prop.getProperty("url")
  val accountGenderSQL =
    """select a.moretvid,b.gender
      |from ucenter.bbs_ucenter_members as a join ucenter.bbs_ucenter_memberfields as b
      |on a.uid=b.uid
      |where a.moretvid is not null""".stripMargin

  val accountAgeSQL =
    """select a.moretvid,b.birthday
      |from ucenter.bbs_ucenter_members as a join ucenter.bbs_ucenter_memberfields as b
      |on a.uid=b.uid
      |where a.moretvid is not null""".stripMargin


  /**
    * Function: obtain data from data table
    *
    * @param url
    * @param sql
    * @param map
    */
  def initMap(url: String, sql: String, map: Map[String, String], userName: String = user, passwordStr: String = password) = {
    try {
      Class.forName(driver)
      val conn: Connection = DriverManager.getConnection(url, userName, passwordStr)
      val stat: Statement = conn.createStatement
      val rs: ResultSet = stat.executeQuery(sql)
      while (rs.next) {
        map += (rs.getString(1) -> rs.getString(2))
      }
      rs.close
      stat.close
      conn.close
    }
    catch {
      case e: Exception => {
        throw new RuntimeException(e)
      }
    }
  }

  def init(): Unit = {

    initMap(url_ucenter_17, accountAgeSQL, accountAgeMap, user, password)

    initMap(url_ucenter_17, accountGenderSQL, accountGenderMap, user, password)

  }

  init()

  /**
    * Function: obtain gender info
    *
    * @param accountId
    * @return
    * 获取注册账号用户的性别
    */
  def getGenderFromAccount(accountId: String) = {
    if (accountGenderMap.isEmpty) {
      initMap(url_ucenter_17, accountAgeSQL, accountAgeMap, user, password)
    }
    accountAgeMap.getOrElse(accountId, "未知")
  }


  /**
    * 获取用户的年龄信息
    *
    * @param accountId
    * @return
    */
  def getAgeFromAccount(accountId: String) = {
    if (accountAgeMap.isEmpty) {
      initMap(url_ucenter_17, accountAgeSQL, accountAgeMap, user, password)
    }
    accountAgeMap.getOrElse(accountId, "未知")
  }

}
