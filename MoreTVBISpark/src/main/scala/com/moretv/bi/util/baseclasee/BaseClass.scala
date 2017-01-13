package com.moretv.bi.util.baseclasee

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.SdkConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by admin on 16/8/8.
 */
trait BaseClass extends LogConfig{
  /**
   * define some parameters
   */
  var sc:SparkContext = null
  implicit var sqlContext:SQLContext = null
  val config = new SparkConf().
    set("spark.executor.memory", "6g").
    set("spark.executor.cores", "3").
    set("spark.scheduler.mode","FAIR").
    set("spark.eventLog.enabled","true").
    set("spark.eventLog.dir","hdfs://hans/spark-log/spark-events").
    set("spark.cores.max", "80").
    set("spark.driver.maxResultSize","2g").
    setAppName(this.getClass.getSimpleName)

  /**
   * initialize global parameters
   */
  def init()={
    sc = new SparkContext(config)
    sqlContext = SQLContext.getOrCreate(sc)
    DataIO.init(SdkConfig.CONFIG_PATH)
    }

  /**
   * this method do not complete.Sub class that extends BaseClass complete this method
   */
  def execute(args: Array[String])

  /**
   * release resource
   */
  def destroy()={
    if(sc!=null) {
      sqlContext.clearCache()
      sc.stop()
    }
  }

}
