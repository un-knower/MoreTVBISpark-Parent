package com.moretv.bi.temp.test

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/4/26.
  */
object PartialDataLoad extends SparkSetting{

  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    /*sqlContext.read.load("/mbi/parquet/playview/20160424").select("userId","contentType").registerTempTable("log_data")
    sqlContext.sql("select contentType,count(distinct userId),count(userId) from log_data " +
      "group by contentType").collect().foreach(println)*/
    sqlContext.read.load("/mbi/parquet/playview/20160424").select("userId").registerTempTable("log_data")
    sqlContext.sql("select count(distinct userId),count(userId) from log_data").collect().foreach(println)
  }
}
