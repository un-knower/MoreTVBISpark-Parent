package com.moretv.bi.temp.login

import java.util.Calendar
import java.lang.{Long => JLong}
import com.moretv.bi.report.medusa.functionalStatistic.appRecommendInstallInfo._
import com.moretv.bi.util.{CodeToNameUtils, DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by 陈佳星 on 2016/9/7.
  *
  */
object activeUserCount extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val appRecommendDir = "/log/moretvloginlog/parquet"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)
          val appRecommendInput = s"$appRecommendDir/$date/*"

          sqlContext.read.parquet(appRecommendInput).registerTempTable("log_data")

          val rdd = sqlContext.sql("select count(distinct mac) from log_data " ).map(e => (e.getLong(0)))

          if(p.deleteOld){
            val deleteSql="delete from medusa_active_user_num where day=?"
            util.delete(deleteSql,insertDate)
          }


          val sqlInsert = "insert into medusa_active_user_num(day,user_num) " +
            "values (?,?)"

          rdd.collect().foreach(e => {
            util.insert(sqlInsert, insertDate,new JLong(e))
          })
        })
      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
