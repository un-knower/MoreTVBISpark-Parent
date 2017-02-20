package com.moretv.bi.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import scala.collection.mutable
import scala.collection.mutable.Map

/**
 * Created by xiajun on 2016/8/2.
 * 用于映射直播的sid与name
 */
object LiveCodeToNameUtils {
  /**
   * 定义一些map集合
   */
  lazy val channelNameMap = initSidMapInfo(url_tvservice_19,channelNameSql)
  lazy val sportNameMap = initSidMapInfo(url_mtv_cms_23,SportNameSql,user1,password1)
  lazy val mvSubjectNameMap = initSidMapInfo(url_mtv_cms_23,mvSubjectNameSql,user1,password1)

  /**
   * 定义一些常量
   */
  val driver:String = "com.mysql.jdbc.Driver"
  val  user:String = "bi"
  val password:String = "mlw321@moretv"

  val user1:String = "bislave"
  val password1:String = "slave4bi@whaley"


  val url_tvservice_19:String = "jdbc:mysql://10.10.2.19:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true"
  val url_mtv_cms_23:String = "jdbc:mysql://10.10.2.23:3306/mtv_cms?useUnicode=true&characterEncoding=utf-8&autoReconnect=true"

  val channelNameSql:String = "SELECT sid,station FROM tvservice.mtv_channel where sid is not null"
  val SportNameSql:String = "SELECT sid,title from mtv_cms.sailfish_sport_match where sid is not null"
  val mvSubjectNameSql:String = "SELECT sid,title from mtv_cms.mtv_mvtopic where sid is not null"


  /**
   * Function: obtain data from data table
   * @param url
   * @param sql
   * @param map
   */
  def initSidMap(url: String, sql: String, map: Map[String, String],userName:String = user,passwordStr:String = password) ={

    this.synchronized{
      Class.forName(driver)
      val conn: Connection = DriverManager.getConnection(url, userName, passwordStr)
      try {

        val stat: Statement = conn.createStatement
        val rs: ResultSet = stat.executeQuery(sql)
        while (rs.next) {
          map += (rs.getString(1) -> rs.getString(2))
        }
        rs.close()
        stat.close()
        conn.close()
      }finally {
        conn.close()
      }
    }
  }

  /**
    * 通过懒加载的方式来初始化
    */
  private def initSidMapInfo(url:String,sql:String,userName:String = user,passwordStr:String = password) = {
    Class.forName(driver)
    val conn:Connection = DriverManager.getConnection(url,userName,passwordStr)
    try{
      val stat:Statement = conn.createStatement
      val rs:ResultSet = stat.executeQuery(sql)
      val map = new mutable.HashMap[String,String]()
      while(rs.next()){
        map.+= (rs.getString(1) -> rs.getString(2))
      }
      rs.close()
      stat.close()
      conn.close()
      map.toMap
    }finally {
      conn.close()
    }
  }


  /**
   * Function: obtain channel name by sid
   * @param sid
   * @return
   * 处理live日志中的sid与name的映射，由于live中的sid会包含
   */
  def getChannelNameBySid(sid:String):String ={
//    if(channelNameMap.isEmpty){
//      initSidMap(url_tvservice_19, channelNameSql, channelNameMap)
//    }
//    if(sportNameMap.isEmpty){
//      initSidMap(url_mtv_cms_23,SportNameSql,sportNameMap,user1,password1)
//    }
    channelNameMap.get(sid) match {
      case Some(title) => title
      case None => sportNameMap.getOrElse(sid,null)
    }

  }

  def getMVSubjectName(sid:String) = {
//    if(mvSubjectNameMap.isEmpty){
//      initSidMap(url_mtv_cms_23,mvSubjectNameSql,mvSubjectNameMap,user1,password1)
//    }
    mvSubjectNameMap.getOrElse(sid,sid)
  }

}
