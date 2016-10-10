package com.moretv.bi.login

import java.lang.{Double => JDouble, Integer => JInt, Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

//统计openApi每天Null渠道，userType=1的新增用户数，以及每天的活跃用户数
object OpenApiUserTrend extends BaseClass{
  val Regex = ("mac=(\\w{12})").r

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(OpenApiUserTrend,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util_bi = new DBOperationUtils("bi")
        val util_tvservice = new DBOperationUtils("tvservice")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
          val apiMac = sc.textFile("/log/moretvloginlog/rawlog/" + date + "/loginlog.access.log_" + date + "-openapi-*").
            map(matchLog).filter(_ != null)
          val user_num = apiMac.distinct().count()
          val access_num = apiMac.count()
          val sqlQuery = "SELECT COUNT(DISTINCT mac) FROM mtv_account " +
            "WHERE openTime >= '"+day+" 00:00:00' AND openTime <= '"+day+" 23:59:59' " +
            "AND promotion_channel IS NULL AND userType = 1"
          val newUserCount = util_tvservice.selectOne(sqlQuery)(0).toString.toLong

          if(p.deleteOld){
            val sqlDelete= "DELETE FROM openApiUserTrend WHERE day = ?"
            util_bi.delete(sqlDelete,day)
          }
          val sqlInsert = "INSERT INTO openApiUserTrend(day,active_user_num,active_access_num,new_user_num) VALUES(?,?,?,?)"
          util_bi.insert(sqlInsert,day,new JLong(user_num),new JLong(access_num),new JLong(newUserCount))
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
    def matchLog(log:String) = {
      Regex findFirstMatchIn log match {
        case Some(x) =>
          x.group(1)
        case None => null
      }
    }
  }
}


