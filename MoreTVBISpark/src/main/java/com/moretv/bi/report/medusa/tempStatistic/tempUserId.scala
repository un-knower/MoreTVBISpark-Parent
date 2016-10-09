package com.moretv.bi.report.medusa.tempStatistic

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext}

/**
 * Created by Administrator on 2016/5/15.
 */
object tempUserId extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p)=>{
        val startDate = p.startDate
        val numOfDays = p.numOfDays
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val fileDir = "/log/medusa/parquet"
        val logType = "enter"

        val dateFormat = new SimpleDateFormat("yyyyMMdd")
        val calendar = Calendar.getInstance()
        calendar.setTime(dateFormat.parse(startDate))
        calendar.add(Calendar.DAY_OF_MONTH,-1)
        val insertDate = DateFormatUtils.readFormat.format(calendar.getTime)
        val inputs = new Array[String](numOfDays)
        val outputPath = s"/log/medusa/updateInfo/$insertDate/userId"
        for (i <- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          inputs(i) = s"$fileDir/$date/$logType/"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }

        val logData = sqlContext.read.parquet(inputs:_*)
        logData.select("userId","apkVersion","productModel").registerTempTable("log")
        val userRdd = sqlContext.sql("select distinct userId from log where apkVersion='3.0.5' and productModel not in " +
          "('MagicBox_M13','M321','LetvNewC1S','we20s')").map(e=>(e.getString(0)))

        val mergerRdd = userRdd.map(e=>(insertDate,e))
        val infoToDf = mergerRdd.toDF("date","userId")
        infoToDf.write.parquet(outputPath)

      }
      case None=>{throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
