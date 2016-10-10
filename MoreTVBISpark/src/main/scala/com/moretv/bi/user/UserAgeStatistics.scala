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
object UserAgeStatistics extends BaseClass with QueryMaxAndMinIDUtil{
  def main(args: Array[String]) {
    config.setAppName("UserAgeStatistics")
    ModuleClass.executor(UserAgeStatistics,args)
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
          "SELECT IFNULL(LEFT(birthday,4),'') FROM `bbs_ucenter_memberfields` WHERE UID >= ? AND UID <= ?",
          id(1), id(0), 10,
          r=>r.getString(1)).map(e=>(matchLog(e))).filter(_!=null).countByKey()

        val util = new DBOperationUtils("bi")
        //delete old data
        if(p.deleteOld) {
          val oldSql = s"delete from user_age_overview where day = '$yesterdayCN'"
          util.delete(oldSql)
        }
        val sql = "INSERT INTO bi.user_age_overview(day,year,age,user_num) values(?,?,?,?)"
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
    if (log.equalsIgnoreCase("") || log.contains("月") || log.contains("日")){
      (("其他","其他"),1)
    }else {
      val yesterday = Calendar.getInstance()
      yesterday.add(Calendar.DAY_OF_MONTH, -1)
      val nowYear = yesterday.get(Calendar.YEAR)
      val age = nowYear - Integer.parseInt(log)
      if(age>0 && age <=19){
        ((log,"19以下"),1)
      }else if(age>19 && age <=29){
        ((log,"20~29"),1)
      }else if(age>29 && age <=39){
        ((log,"30~39"),1)
      }else if(age>39 && age <=49){
        ((log,"40~49"),1)
      }else if(age>49){
        ((log,"50以上"),1)
      }else null
    }
  }
}
