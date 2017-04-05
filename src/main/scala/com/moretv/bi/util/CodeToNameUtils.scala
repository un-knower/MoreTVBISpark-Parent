package com.moretv.bi.util

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by laishun on 2015/7/17.
 */
@deprecated
object CodeToNameUtils {
  /**
   * 定义一些map集合
   */
  lazy val sidApplicationNameMap = initSidMapInfo(db1, sid2ApplicationNameMapSql)
  lazy val sidSubjectNameMap = initSidMapInfo(db1, sid2SubjectNameMapSql)
  lazy val subjectName2CodeMap = initSidMapInfo(db1, subjectName2CodeMapSql)
  lazy val thirdPaheNameMap = initSidMapInfo(db2,thirdPathSql)
  lazy val subCodeToParentNameMap = initSidMapInfo(db2,subCodeToParentNameSql)
  lazy val channelNameMap = initSidMapInfo(db1,channelNameSql)
  lazy val tmpSubChannelNameMap = initSidMapInfo(db1,subChannelNameSql)
  lazy val sidProgramNameMap = initSidMapInfo(db1,programSid2NameMapSql)
  lazy val sidProgramDurationMap = initSidMapInfo(db1,programSid2DurationMapSql)
  /**
   * 定义一些常量
   */
  val db1 = DataIO.getMySqlOps(DataBases.MORETV_RECOMMEND_TVSERVICE_MYSQL)
  val db2 = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)


   val sid2ApplicationNameMapSql:String = "SELECT sid,title FROM tvservice.mtv_application WHERE sid IS NOT NULL"
   val sid2SubjectNameMapSql:String = "SELECT code,name FROM tvservice.mtv_subject WHERE code IS NOT NULL"
   val subjectName2CodeMapSql:String = "SELECT name,code FROM tvservice.mtv_subject WHERE name IS NOT NULL"
   val thirdPathSql:String = "SELECT code,name FROM bi.mtv_list"
   val subCodeToParentNameSql:String = "SELECT code,parent_code FROM bi.mtv_list where hierarchy = 3"
   val channelNameSql:String = "SELECT sid,station FROM tvservice.mtv_channel where sid is not null"
   val subChannelNameSql:String = "SELECT code,name FROM tvservice.mtv_position"
   val programSid2NameMapSql:String = "SELECT sid,title FROM tvservice.mtv_program WHERE sid IS NOT NULL"
   val programSid2DurationMapSql:String = "SELECT sid,duration FROM tvservice.mtv_program WHERE sid IS NOT NULL"



  /**
    * 使用懒加载的方式进行初始化
    */
  private def initSidMapInfo(db:MySqlOps, sql:String) = {
    val map = new mutable.HashMap[String,String]()
    val result = db.selectArrayList(sql)
    result.foreach(rs => {
      map.+= (rs(0).toString -> rs(1).toString)
    })
    map.toMap
  }
  /**
   * Function: obtain subject name by subject code
   * @param sid
   * @return
   */
  def getSubjectNameBySid(sid: String):String ={
    sidSubjectNameMap.getOrElse(sid,sid)
  }

  def getAllSubjectName:collection.Map[String,String] = {
    sidSubjectNameMap
  }

  /**
   * Function: obtain program name by program sid
   * @param sid
   * @return
   */

  def getProgramNameBySid(sid:String):String = {
    sidProgramNameMap.getOrElse(sid,"null")
  }

  /**
    * Function: obtain subject name by subject code
    * @param name
    * @return
    */
  def getSubjectCodeByName(name: String):String ={
    subjectName2CodeMap.getOrElse(name,"null")
  }

  def getAllSubjectCode():collection.Map[String,String] = {
    subjectName2CodeMap
  }

  /**
   * Function: obtain apllication name by sid
   * @param sid
   * @return
   */
  @deprecated
  def getApplicationNameBySid(sid: String):String ={
    sidApplicationNameMap.getOrElse(sid,"null")
  }

  /**
   * Function: obtain path name by path code
   * @param sid
   * @return
   */
  def getThirdPathName(sid:String):String ={
    thirdPaheNameMap.getOrElse(sid,"null")
  }

  /**
   * Function: obtain channel name by sid
   * @param sid
   * @return
   */
  def getChannelNameBySid(sid:String):String ={
    channelNameMap.getOrElse(sid,null)
  }

  /**
   * Function: obtain the name of parent's path by sub path code
   * @param code
   * @return
   */
  def getParentNameBySubCode(code:String): String ={
    var parentName = subCodeToParentNameMap.getOrElse(code,"")
    if("".equalsIgnoreCase(parentName)){
      parentName = code
    }
    parentName
  }

  def getProgramDurationFromSid(sid:String) ={
    sidProgramDurationMap.getOrElse(sid,"null")
  }


  def getSubjectCodeMap:scala.collection.immutable.Map[String,String] = {
    subjectName2CodeMap
  }
}
