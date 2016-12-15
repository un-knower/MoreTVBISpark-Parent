package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/23.
 */
object medusaTotalUserFromAllLogInfo extends SparkSetting{

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "6g").
          set("spark.executor.cores", "10")
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate

        val medusaDir = "/log/medusa/parquet"
        val outputPath = s"/log/medusa/mac2UserId/$startDate/userIdFromAllLog"


        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        val insertDate = DateFormatUtils.toDateCN(DateFormatUtils.readFormat.format(calendar.getTime),-1)


        val inputs = new Array[String](p.numOfDays)
        for(i <- 0 until p.numOfDays){
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          inputs(i) = s"$medusaDir/$date/*"
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
        val medusaLog = sqlContext.read.parquet(inputs:_*)
        medusaLog.select("userId","apkVersion").registerTempTable("medusa_total_log")
        val medusaTotalUserRdd = sqlContext.sql("select distinct userId from medusa_total_log where apkVersion='3.0" +
          ".6'").map(e=>e.getString(0))

        val medusaTotalUserDF = medusaTotalUserRdd.toDF("userId")
        medusaTotalUserDF.write.parquet(outputPath)


      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
