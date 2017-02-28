package com.moretv.bi.util

import org.apache.spark.SparkConf

/**
 * Created by mycomputer on 2015/3/27.
 */
trait SparkSetting {

  val tachyonMaster = ""
  val config = new SparkConf().
    set("spark.executor.memory", "3g").
    set("spark.executor.cores", "3").
    set("spark.scheduler.mode","FAIR").
    set("spark.eventLog.enabled","true").
    set("spark.speculation","true").
    set("spark.eventLog.dir","hdfs://hans/spark-log/spark-events").
    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
    set("spark.kryo.registrator", "com.moretv.bi.util.MyRegistrator").
    set("spark.cores.max", "72").
    setAppName(this.getClass.getSimpleName)

}
