package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.lang.{Long=>JLong}
import com.moretv.bi.util.{SparkSetting, DateFormatUtils, DBOperationUtils, ParamsParseUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/13.
 */
object medusaRetentionRate extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p)=>{
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")
        val logType = "enter"
        // 确定今天的时间
        val currentDay = new Date()
        val dateFormat = new SimpleDateFormat("yyyyMMdd")
        val today = dateFormat.format(currentDay)
        // 参数为分析的是哪一天的留存率
        val analysisDay = p.startDate
        val cal1 = Calendar.getInstance()
        cal1.setTime(DateFormatUtils.readFormat.parse(analysisDay))
        val date = DateFormatUtils.toDateCN(DateFormatUtils.readFormat.format(cal1.getTime))
        val cal2 = Calendar.getInstance()
        cal2.setTime(DateFormatUtils.readFormat.parse(today))
        val day1 = cal1.get(Calendar.DAY_OF_YEAR)
        val day2 = cal2.get(Calendar.DAY_OF_YEAR)
        // 分析的时间与今天的时间之间的差值
        val numOfDays = day2-day1
        val analysisDayPath = s"/log/medusa/updateInfo/$analysisDay/userId"
        cal1.add(Calendar.DAY_OF_MONTH,-1)
        val previousDay = DateFormatUtils.readFormat.format(cal1.getTime)
        val previousDayPath = s"/log/medusa/updateInfo/$previousDay/userId"
        sqlContext.read.parquet(analysisDayPath).select("userId").registerTempTable("userId1")
        sqlContext.read.parquet(previousDayPath).select("userId").registerTempTable("userId2")
        val analysisUserIdRdd  =sqlContext.sql("select userId from userId1").map(e=>e.getString(0))
        val previousUserIdArr = sqlContext.sql("select userId from userId2").map(e=>e.getString(0)).collect()
        // 获取当日更细的userId信息
        val newUpdateUserIdRdd = analysisUserIdRdd.filter(!previousUserIdArr.contains(_))
        val newUpdateNum = newUpdateUserIdRdd.collect().length
        /*回到analysis的日期*/
        cal1.add(Calendar.DAY_OF_MONTH,1)
        println(numOfDays)
        (1 until numOfDays).foreach(i=>{
          cal1.add(Calendar.DAY_OF_MONTH,1)
          val day = DateFormatUtils.readFormat.format(cal1.getTime)
          // 获取分析日后面几天的登录信息
          val loginPath = s"/log/medusa/parquet/$day/$logType"
          sqlContext.read.parquet(loginPath).select("userId","apkVersion").registerTempTable("loginUserId")
          val loginUserIdRdd = sqlContext.sql("select distinct userId from loginUserId where apkVersion='3.0.5'")
            .map(e=>e.getString(0))

          val intersectionUserIdRdd = newUpdateUserIdRdd.intersection(loginUserIdRdd)

          val retentionUserNum = intersectionUserIdRdd.collect().length

          val sqlInsert = "insert into medusa_gray_testing_retention_rate(date,newUpdateNum,retentionFlag,retentionNum) values" +
            " (?,?,?,?)"

          util.insert(sqlInsert,date,new JLong(newUpdateNum),i.toString,new JLong(retentionUserNum))

        })
      }
      case None=>{throw new RuntimeException("Needs the day startDate Info!")}
    }
  }
}
