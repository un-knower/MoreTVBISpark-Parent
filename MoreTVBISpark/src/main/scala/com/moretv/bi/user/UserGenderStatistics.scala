package com.moretv.bi.user

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
 * Created by Will on 2015/4/18.
 */
object UserGenderStatistics extends BaseClass with QueryMaxAndMinIDUtil{
  def main(args: Array[String]) {
    config.setAppName("UserGenderStatistics")
    ModuleClass.executor(UserGenderStatistics,args)
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
          "SELECT IFNULL(gender,'0') FROM `bbs_ucenter_memberfields` WHERE UID >= ? AND UID <= ?",
          id(1), id(0), 10,
          r=>r.getString(1)).map(e=>(matchLog(e))).countByKey().toMap

        val female=resultRDD.getOrElse("f",0L)
        val male=resultRDD.getOrElse("m",0L)
        val others=resultRDD.getOrElse("o",0L)
        val empty = resultRDD.getOrElse("0",0L)

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val oldSql = s"delete from user_gender_overview where day = '$yesterdayCN'"
          util.delete(oldSql)
        }
        val sql = "INSERT INTO bi.user_gender_overview(day,female,male,others,empty) values(?,?,?,?,?)"
        util.insert(sql,yesterdayCN,new Integer(female.toInt),new Integer(male.toInt),new Integer(others.toInt),new Integer(empty.toInt))
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def matchLog(log:String) = {
    if (log.equalsIgnoreCase("")|| log.equalsIgnoreCase("0")){
      ("0",1)
    }else if (log.equalsIgnoreCase("f")){
      ("f",1)
    }else if (log.equalsIgnoreCase("m")){
      ("m",1)
    }else if(log.equalsIgnoreCase("o")){
      ("o",1)
    }else ("0",1)
  }
}
