package com.moretv.bi.report.test

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.tempStatistic.MedusaplayInfoModel._
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by 王宝志 on 2016/9/12.
  * 最近一周各渠道新增用户的终端品牌分布（需要排除掉非OTT设备）
  * /log/whaley/parquet/20160911/appiu 存储20160911全天的数据
  */
object ChannelNUUTerminalDistribution {
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "6g").
          set("spark.cores.max", "100").setAppName("AppInstallTop200-wangbaozhi")
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val medusaDir = "/log/whaley/parquet/"
        //val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i => {
          val dateTime = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"$medusaDir/$dateTime/appiu/"
          println("-----inputPath is " + inputPath)
          //val day = DateFormatUtils.toDateCN(dateTime,-1)
          sqlContext.read.load(inputPath).select("appPackage", "event").registerTempTable("appiu_table")

          //查询
          val userByAreaAndISP = sqlContext.sql("select appPackage,count(appPackage) as total_count from appiu_table where event='install' group by appPackage order by total_count desc limit 200").map(e => (e.getString(0),e.getLong(1)))

          //打印出结果
          userByAreaAndISP.collect().foreach(println)
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At needs the param: startDate!")
      }
    }

  }

}

/**
  *
  *
nohup /hadoopecosystem/spark/bin/spark-submit --master spark://10.10.2.14:7077  \
       --conf "spark.executor.extraJavaOptions=-XX:+UseParNewGC -XX:+UseConcMarkSweepGC \
       -XX:-CMSConcurrentMTEnabled -XX:CMSInitiatingOccupancyFraction=70 \
       -XX:+CMSParallelRemarkEnabled"   \
       --class com.moretv.bi.report.test.AppInstallTop200 ./MoreTVBISpark.jar  \
       --startDate 20160911 --numOfDays 2 >appInstallTop200.log 2>&1 &

  *
  *
  *
  */
