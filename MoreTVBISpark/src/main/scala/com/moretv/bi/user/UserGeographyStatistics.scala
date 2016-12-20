package com.moretv.bi.user

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
 * Created by Will on 2015/4/18.
 */
object UserGeographyStatistics extends BaseClass with QueryMaxAndMinIDUtil{
  def main(args: Array[String]) {
    config.setAppName("UserGeographyStatistics")
    ModuleClass.executor(UserGeographyStatistics,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val yesterdayCN = DateFormatUtils.toDateCN(p.startDate,-1)
        val id = queryID("uid","bbs_ucenter_memberfields","jdbc:mysql://10.10.2.17:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true");

        val resultRDD = new JdbcRDD(sc, ()=>{
          Class.forName("com.mysql.jdbc.Driver")
          DriverManager.getConnection("jdbc:mysql://10.10.2.17:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
        },
          "SELECT IFNULL(area,'') FROM `bbs_ucenter_memberfields` WHERE UID >= ? AND UID <= ?",
          id(1), id(0), 10,
          r=>r.getString(1)).map(e=>(matchLog(e))).filter(_!=null).countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val oldSql = s"delete from user_geography_overview where day = '$yesterdayCN'"
          util.delete(oldSql)
        }
        val sql = "INSERT INTO bi.user_geography_overview(day,province,city,user_num) values(?,?,?,?)"
        resultRDD.foreach(x => {
          util.insert(sql,yesterdayCN,x._1._1,x._1._2,new Integer(x._2.toInt))
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
