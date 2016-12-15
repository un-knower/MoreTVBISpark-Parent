package com.moretv.bi.report.medusa.userDevelop

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
/**
  * Created by 陈佳星 on 2016/8/23.
  * 统计新增用户渠道分布占比：统计各个渠道新增的用户量占总新增用户量的比例。
  * 例如，按天查询时，则显示各个渠道当天新增的用户量占总当天总新增用户量的比例。
  * 中间结果表：medusa_newuser_from_channel_and_all_info
  */
object newUserCountInfo extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(newUserCountInfo,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val dbsnapshotDir = "/log/dbsnapshot/parquet"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, 0)
          calendar.add(Calendar.DAY_OF_MONTH, -1)
          val newUserInput = s"$dbsnapshotDir/$date/moretv_mtv_account"

          sqlContext.read.parquet(newUserInput).registerTempTable("log_data")

          val rdd = sqlContext.sql(s"select promotion_channel, count(distinct user_id) as user_num from log_data " +
            s"where subString(openTime,0,10)= '$insertDate' and promotion_channel is not null group by promotion_channel " +
            s"order by user_num desc").map(e => (e.getString(0), e.getLong(1)))

          val sqlInsert = "insert into medusa_newuser_from_channel_and_all_info(day,promotion_channel,user_num) " +
            "values (?,?,?)"

          rdd.collect().foreach(e => {
            util.insert(sqlInsert, insertDate, e._1, new JLong(e._2))
          })
        })
      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}