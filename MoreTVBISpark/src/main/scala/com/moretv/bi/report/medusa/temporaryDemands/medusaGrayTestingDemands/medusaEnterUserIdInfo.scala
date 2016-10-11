package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util.{DateFormatUtils, SparkSetting, ParamsParseUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/17.
 */
object medusaEnterUserIdInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc =new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)

        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        val dateFormat = new SimpleDateFormat("yyyyMMdd")
        val logType = "enter"
        val fileDir = "/log/medusa/parquet"



        (0 until p.numOfDays).foreach(i=>{
          val day = DateFormatUtils.readFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val insertDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val logPath = s"$fileDir/$day/$logType"
          val outputDir = s"/log/medusa/temp/$insertDate/enterLogUserId"
          sqlContext.read.parquet(logPath).select("userId").registerTempTable("log")
          val userIdDf = sqlContext.sql("select distinct userId from log")
          userIdDf.write.parquet(outputDir)


        })
      }
      case None=> {throw new RuntimeException("At least needs one startDate: startDate")}
    }
  }

}
