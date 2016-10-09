package com.moretv.bi.kidslogin

import java.util.Calendar

import com.moretv.bi.constant.LogKey._
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/8/20.
 */
object LoginLog2Parquet extends BaseClass{

  private val outputPartionNum = 10

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "2g").
      set("spark.cores.max", "30").
      set("spark.storage.memoryFraction", "0.6")
    ModuleClass.executor(LoginLog2Parquet,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        val deleteOld = p.deleteOld

        (0 until p.numOfDays).foreach(i => {

          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/mtvkidsloginlog/rawlog/mtvkidsloginlog.access.log_$inputDate*"
          val outputPath = s"/log/mtvkidsloginlog/parquet/$inputDate/loginlog"

          if (deleteOld) {
            HdfsUtil.deleteHDFSFile(outputPath)
          }

          val jsonRdd = sc.textFile(inputPath).map(LogUtils.loginlog2json).filter(_ != null).
            map(json => {
              json.put(LOG_TYPE,"loginlog")
              json.toString
            })

          sqlContext.read.json(jsonRdd).coalesce(outputPartionNum).write.parquet(outputPath)

          cal.add(Calendar.DAY_OF_MONTH, -1)

        })

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

}
