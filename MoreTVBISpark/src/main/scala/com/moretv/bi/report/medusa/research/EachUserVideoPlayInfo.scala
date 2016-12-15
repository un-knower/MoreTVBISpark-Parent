package com.moretv.bi.report.medusa.research

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by xiajun on 2016/5/16.
 * 统计每个用户播放每个节目的播放情况！
 * 保存到HDFS中
 */
object EachUserVideoPlayInfo extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "5g").
          set("spark.executor.cores", "5").
          set("spark.cores.max", "100")
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val medusaDir = "/log/medusaAndMoretvMerger/"
        val outputDir = "/log/medusa/research"
        val outType = "eachUserEachVideoPlayInfo"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)
          val outputPath = s"$outputDir/$date/$outType"
          val playviewInput = s"$medusaDir/$date/playview/"

          sqlContext.read.parquet(playviewInput).select("userId","event","videoSid")
            .registerTempTable("log_data")

          val searchRdd = sqlContext.sql("select userId,videoSid,count(userId) from log_data where event in ('startplay'," +
            "'playview') group by userId,videoSid").map(e=>(insertDate,e.getString(0),e.getString(1),e.getLong(2)))

          if (p.deleteOld) {
            HdfsUtil.deleteHDFSFile(outputPath)
          }

          val df = searchRdd.toDF("date","userId","videpSid","playNumber")
          df.write.parquet(outputPath)

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
