package com.moretv.bi.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import scala.collection.mutable.Map

/**
 * Created by xiajun on 2016/8/2.
 * 用于获取账户的相关信息
 */
object AccountIDInfoUtils {
  /**
   * 定义一些map集合
   */
   var accountGenderMap = Map[String,String]()
   var accountAgeMap = Map[String,String]()

  /**
   * 定义一些常量
   */
   val driver:String = "com.mysql.jdbc.Driver"
   val  user:String = "bi"
   val password:String = "mlw321@moretv"



   val url_ucenter_17:String = "jdbc:mysql://10.10.2.19:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true"

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
   * @param url
   * @param sql
   * @param map
   */
  def initMap(url: String, sql: String, map: Map[String, String],userName:String = user,passwordStr:String = password) ={
    try {
      Class.forName(driver)
      val conn: Connection = DriverManager.getConnection(url, userName, passwordStr)
      val stat: Statement = conn.createStatement
      val rs: ResultSet = stat.executeQuery(sql)
      while (rs.next) {
        map +=(rs.getString(1) -> rs.getString(2))
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


  /**
   * Function: obtain gender info
   * @param accountId
   * @return
   * 获取注册账号用户的性别
   */
  def getGenderFromAccount(accountId:String) = {
    if(accountGenderMap.isEmpty){
      initMap(url_ucenter_17,accountAgeSQL,accountAgeMap,user,password)
    }
    accountAgeMap.getOrElse(accountId,"未知")
  }


  /**
   * 获取用户的年龄信息
   * @param accountId
   * @return
   */
  def getAgeFromAccount(accountId:String) = {
    if(accountAgeMap.isEmpty){
      initMap(url_ucenter_17,accountAgeSQL,accountAgeMap,user,password)
    }
    accountAgeMap.getOrElse(accountId,"未知")
  }

}
