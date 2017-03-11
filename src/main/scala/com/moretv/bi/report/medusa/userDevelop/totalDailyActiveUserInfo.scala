package com.moretv.bi.report.medusa.userDevelop

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object totalDailyActiveUserInfo extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate

        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val medusaDailyActiveInput =DataIO.getDataFrameOps.getPath(MEDUSA,"*",date)
          val moretvDailyActiveInput =DataIO.getDataFrameOps.getPath(MORETV,"*",date)

          val medusaDailyActivelog = sqlContext.read.parquet(medusaDailyActiveInput).select("userId","apkVersion")
            .registerTempTable("medusa_daily_active_log")
          val moretvDailyActivelog = sqlContext.read.parquet(moretvDailyActiveInput).select("userId","apkVersion")
            .registerTempTable("moretv_daily_active_log")

          val totalActiveUser=sqlContext.sql("select count(distinct a.userId) from (select distinct userId from " +
            "medusa_daily_active_log Union select distinct userId from moretv_daily_active_log) as a").map(e=>e.getLong(0))

          val sqlInsert = "insert into medusa_user_develop_total_active_user_info(day,active_user) values " +
            "(?,?)"

          if(p.deleteOld){
            val sqlDelete = s"delete from medusa_user_develop_total_active_user_info where day = ?"
            util.delete(sqlDelete,insertDate)
          }

          totalActiveUser.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,new JLong(e))
          })

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
