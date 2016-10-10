package com.moretv.bi.temp.log

import com.moretv.bi.util.SparkSetting
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by Will on 2015/8/10.
 */
object LogApart extends SparkSetting{

  private val regex = "log=(\\w+)-".r
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "3g").
      set("spark.cores.max", "60").
      set("spark.storage.memoryFraction","0.6")

    val sc = new SparkContext(config)
    val inputPath = args(0)
    val outputPath = args(1)
    val logRdd = sc.textFile(inputPath).map(matchLog).filter(_ != null).repartition(1000).
      persist(StorageLevel.MEMORY_AND_DISK_SER)
    val logTypeSet = logRdd.map(_._1).distinct().collect()
    logTypeSet.foreach(logType => {
      logRdd.filter(_._1 == logType).map(_._2).coalesce(64).
        saveAsTextFile(outputPath+logType,classOf[BZip2Codec])
    })
    logRdd.unpersist()
  }

  def matchLog(log:String) = {
    regex findFirstMatchIn log match {
      case Some(m) => (m.group(1),log)
      case None => null
    }
  }
}
