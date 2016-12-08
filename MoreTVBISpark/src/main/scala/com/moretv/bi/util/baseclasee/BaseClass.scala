package com.moretv.bi.util.baseclasee

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
 * Created by admin on 16/8/8.
 */
trait BaseClass{
  /**
   * define some parameters
   */
  var sc:SparkContext = null
  implicit var sqlContext:SQLContext = null
  val config = new SparkConf().
    set("spark.executor.memory", "4g").
    set("spark.executor.cores", "3").
    set("spark.scheduler.mode","FAIR").
    set("spark.eventLog.enabled","true").
    set("spark.eventLog.dir","hdfs://hans/spark-log/spark-events").
    set("spark.cores.max", "72").
    set("spark.driver.maxResultSize","2g").
    setAppName(this.getClass.getSimpleName)

  /**
   * initialize global parameters
   */
  def init()={
    sc = new SparkContext(config)
    sqlContext = SQLContext.getOrCreate(sc)
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
