package com.moretv.bi.temp.medusa

import java.util.Calendar

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by Will on 2015/8/20.
 */
object Log2ParquetUpgrade extends SparkSetting{

  private val repartitionNum = 100
  private val outputPartionNum = 10
  private val regex = "^\\w{1,30}$".r

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
        val inputPath = s"/log/moretvupgrade/rawlog/upgradelog.access.log_$inputDate*"
        val outputPath = s"/log/moretvupgrade/parquet/$inputDate/"

        val logRdd = sc.textFile(inputPath).repartition(repartitionNum).
          map(log => {
            val json = LogUtils.loginlog2json(log)
            if(json == null) null else json.toString
          }).filter(_ != null).persist(StorageLevel.MEMORY_AND_DISK)

        if (p.deleteOld) {
          HdfsUtil.deleteHDFSFile(outputPath)
        }
          sqlContext.read.json(logRdd).coalesce(outputPartionNum).write.parquet(outputPath)

          logRdd.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

}
