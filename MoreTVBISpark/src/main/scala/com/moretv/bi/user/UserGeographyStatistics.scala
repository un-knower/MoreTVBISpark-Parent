package com.moretv.bi.user

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.DataBases
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by Will on 2015/4/18.
 */
object UserGeographyStatistics extends BaseClass with QueryMaxAndMinIDUtil{
  def main(args: Array[String]) {
    config.setAppName("UserGeographyStatistics")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val yesterdayCN = DateFormatUtils.toDateCN(p.startDate,-1)
        val util = DataIO.getMySqlOps(DataBases.MORETV_UCENTER_MYSQL)
        val url = util.prop.getProperty("url")
        val driver = util.prop.getProperty("driver")
        val user = util.prop.getProperty("user")
        val password = util.prop.getProperty("password")
        val id = queryID("uid",Tables.BBS_UCENTER_MEMBERFIELDS,url)
        val sqlInfo = "SELECT IFNULL(area,'') FROM `bbs_ucenter_memberfields` WHERE UID >= ? AND UID <= ?"

        val resultRDD = MySqlOps.getJdbcRDD(sc,sqlInfo,Tables.BBS_UCENTER_MEMBERFIELDS,
          r=>r.getString(1),driver,url,user,password,(id(1), id(0)),10).
          map(e=>(matchLog(e))).filter(_!=null).countByKey()

        val db = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val oldSql = s"delete from user_geography_overview where day = '$yesterdayCN'"
          db.delete(oldSql)
        }
        val sql = "INSERT INTO bi.user_geography_overview(day,province,city,user_num) values(?,?,?,?)"
        resultRDD.foreach(x => {
          db.insert(sql,yesterdayCN,x._1._1,x._1._2,new Integer(x._2.toInt))
        })
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def matchLog(log:String) = {
    if (log.equalsIgnoreCase("")){
      (("其他","其他"),1)
    }else {
      var temp:Array[String] = null
      if(log.contains(",") && !log.contains(";")){
        temp = log.split(",")
      }else if(!log.contains(",") && log.contains(";")){
        temp = log.split(";")
      }else if(log.contains(",") && log.contains(";")){
        val arr1 = log.split(";")
        val arr2 = log.split(",")
        if(arr1.length > arr2.length)
          temp = arr1
        else temp = arr2
      }

      if (temp.length < 0 || temp.length == 0){
        (("其他","其他"),1)
      }else if (temp.length == 1){
        ((temp(0),temp(0)),1)
      }else if (temp.length  > 1){
        if (temp(1).equalsIgnoreCase("市辖区") || temp(1).equalsIgnoreCase("")) ((temp(0),temp(0)),1) else ((temp(0),temp(1)),1)
      }else null
    }
  }
}
