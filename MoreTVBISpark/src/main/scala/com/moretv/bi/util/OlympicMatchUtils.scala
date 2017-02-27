package com.moretv.bi.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases

import scala.collection.mutable.Map

/**
 * Created by xiajun on 2016/8/2.
 * 用于映射直播的sid与name
 */
object OlympicMatchUtils {
  /**
   * 定义一些map集合
   */
   var olympicMatchMap = Map[String,String]()

  /**
   * 定义一些常量
   */
   val driver:String = "com.mysql.jdbc.Driver"
   val db = DataIO.getMySqlOps(DataBases.MORETV_CMS_MYSQL)
   val user:String = db.prop.getProperty("user")
   val password:String = db.prop.getProperty("password")

   val url_mtv_cms_23:String = db.prop.getProperty("url")

   val olympicMatchSql:String = "SELECT sid,league_id from mtv_cms.sailfish_sport_match where sid is not null"


  /**
   * Function: obtain data from data table
   * @param url
   * @param sql
   * @param map
   */
  def initSidMap(url: String, sql: String, map: Map[String, String],userName:String,passwordStr:String) ={
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
   * Function: obtain channel name by sid
   * @param sid
   * @return
   * 获取体育赛事及联赛ID
   */
  def getMatchLeague(sid:String):String ={
    if(olympicMatchMap.isEmpty){
      initSidMap(url_mtv_cms_23, olympicMatchSql, olympicMatchMap,user,password)
    }
    olympicMatchMap.getOrElse(sid,null)
  }
}
