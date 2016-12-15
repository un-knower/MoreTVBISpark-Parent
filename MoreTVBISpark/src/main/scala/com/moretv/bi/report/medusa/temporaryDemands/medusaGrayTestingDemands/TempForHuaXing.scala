package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 *
 */
object TempForHuaXing extends SparkSetting {
  def main(args: Array[String]) {
    config.   set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "120")
    val sc = new SparkContext(config)
    implicit val sqlContext = new SQLContext(sc)
    val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
    val startArr = Array("2016-10-01","2016-09-01","2016-08-01","2016-07-01","2016-06-01",
      "2016-05-01", "2016-04-01", "2016-03-01", "2016-02-01", "2016-01-01",
      "2015-12-01", "2015-11-01", "2015-10-01", "2015-09-01", "2015-08-01", "2015-07-01",
      "2015-06-01", "2015-05-01", "2015-04-01", "2015-03-01", "2015-02-01", "2015-01-01",
      "2014-12-01", "2014-11-01", "2014-10-01", "2014-09-01", "2014-08-01", "2014-07-01",
      "2014-06-01", "2014-05-01", "2014-04-01", "2014-03-01", "2014-02-01", "2014-01-01",
      "2013-12-01")
    val endArr = Array("2016-10-31","2016-09-30","2016-08-31","2016-07-31","2016-06-30", "2016-05-31", "2016-04-30", "2016-03-31", "2016-02-29", "2016-01-31",
      "2015-12-31", "2015-11-30", "2015-10-31", "2015-09-30", "2015-08-31", "2015-07-31",
      "2015-06-30", "2015-05-31", "2015-04-30", "2015-03-31", "2015-02-29", "2015-01-31",
      "2014-12-31", "2014-11-30", "2014-10-31", "2014-09-30", "2014-08-31", "2014-07-31",
      "2014-06-30", "2014-05-31", "2014-04-30", "2014-03-31", "2014-02-29", "2014-01-31",
      "2013-12-31")
    val arr = "201610*"
    (0 until 21).foreach(i=>{

      val dir = "/log/dbsnapshot/parquet/20161101/moretv_mtv_account"
      sqlContext.read.parquet(dir).registerTempTable("log")
      val startDay = startArr(i)
      val endDay = endArr(i)
      val startTime = s"$startDay"+" "+"00:00:00"
      val endTime = s"$endDay"+" "+"23:59:59"
      val rdd = sqlContext.sql(s"select distinct user_id from log where openTime between '$startTime' and '$endTime'").map(
      e=>e.getString(0)
      )
      val insertSql = "insert into temp_huaxing(month,month1,num) values (?,?,?)"

      if(i>0){
          val dir1 = s"/log/medusaAndMoretvMerger/$arr/*"
          sqlContext.read.parquet(dir1).registerTempTable("log1")
          val rdd1 = sqlContext.sql("select distinct userId from log1").map(e=>e.getString(0))
          val num = rdd.intersection(rdd1).count()
          util.insert(insertSql,startDay.take(7),arr,new JLong(num))
      }
    })


  }
}
