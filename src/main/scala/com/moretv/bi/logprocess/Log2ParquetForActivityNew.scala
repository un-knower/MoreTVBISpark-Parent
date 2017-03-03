/**
 * created by zhangyu on 2016/9/18
 * 将活动原始日志转成parquet日志,不区分logtype
 */

package com.moretv.bi.logprocess

import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

object Log2ParquetForActivityNew extends BaseClass{

  private val outputPartionNum = 10
  //private val regex = "\\w{1,30}".r

  def main(args: Array[String]) {
//    config.set("spark.executor.memory", "3g").
//      set("spark.cores.max", "100").
//      set("spark.storage.memoryFraction", "0.6")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        val deleteOld = p.deleteOld

        (0 until p.numOfDays).foreach(i => {

          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"/log/activity/rawlog/activity.access.log.$inputDate-*"
          val outputPath = s"/log/activity/parquet/$inputDate/"

          val logRdd = sc.textFile(inputPath).map(row=>{
            val logJsonStr = LogUtils.logProcessForActivity(row)
            logJsonStr
          }).filter(_ != null)

          if (deleteOld) {
            HdfsUtil.deleteHDFSFile(outputPath)
          }
          sqlContext.read.json(logRdd).coalesce(outputPartionNum).write.parquet(outputPath)

          cal.add(Calendar.DAY_OF_MONTH, -1)

          //logRdd.unpersist()
        })

      }
      case None =>
        throw new RuntimeException("At least need param --startDate.")

    }
  }

}
