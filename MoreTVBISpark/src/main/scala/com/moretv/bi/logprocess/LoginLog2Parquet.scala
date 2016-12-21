package com.moretv.bi.logprocess

import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.moretv.bi.constant.LogKey._

/**
 * Created by Will on 2015/8/20.
 */
object LoginLog2Parquet extends BaseClass{

  private val outputPartionNum = 10

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val s = sqlContext
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        val deleteOld = p.deleteOld

        (0 until p.numOfDays).foreach(i => {

          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/moretvloginlog/rawlog/$inputDate/*"
          val outputPath = s"/log/moretvloginlog/parquet/$inputDate/loginlog"

          if (deleteOld) {
            HdfsUtil.deleteHDFSFile(outputPath)
          }

          val jsonRdd = sc.textFile(inputPath).map(LogUtils.logProcessLogin).filter(_ != null)

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
