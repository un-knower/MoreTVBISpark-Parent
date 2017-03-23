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
  val config = new SparkConf()
  DataIO.init(SdkConfig.CONFIG_PATH)
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
