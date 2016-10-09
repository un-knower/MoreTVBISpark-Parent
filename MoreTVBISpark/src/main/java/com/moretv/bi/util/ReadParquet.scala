package com.moretv.bi.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/9/24.
 */
object ReadParquet extends SparkSetting{

  def main(args: Array[String]) {
    val path = args(0)
    val num = args(1).toInt
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    sqlContext.read.load(path).show(num)
    sc.stop()
  }
}
