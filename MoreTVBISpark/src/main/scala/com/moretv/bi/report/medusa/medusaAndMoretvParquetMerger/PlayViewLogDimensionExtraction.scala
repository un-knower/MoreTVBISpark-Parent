package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.UDFConstantDimension
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by wangbaozhi on 2016/12/26,
  * This object is used to generate dimension table and exchange the primary key from dimension table and fat table
  * input: /log/medusaAndMoretvMergerDimension/$date/playview2filter
  * output:/data_warehouse/dimensions/medusa/
  *
  * 1.generate dimension table from big fat table
  * 2.exchange
  *
  * take source_list（列表页分类入口） for example
  */

//not used,it can be instead of PlayViewLogDimensionExchange
object PlayViewLogDimensionExtraction extends BaseClass {
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "10g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "20")
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    println("-------------------------in execute--------------")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val startDate = p.startDate
        val inputDirBase = "/log/medusaAndMoretvMergerDimension"
        val inputLogType = "playview2filter"
        val outputDirBase = "/data_warehouse/dimensions/medusa/daily"
        val outputType = "sourceList"
        val unique_key=UDFConstantDimension.SOURCE_LIST_SK

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i => {
          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val inputDir = s"$inputDirBase/$inputDate/$inputLogType"
          val inputDirFlag = FilesInHDFS.IsInputGenerateSuccess(inputDir)
          val outputPath = s"$outputDirBase/$inputDate/$outputType"
          println("inputDate"+inputDate+",inputDir:" + inputDir)
          println("inputDirFlag"+inputDirFlag+",outputPath:" + outputPath)

          if (inputDirFlag) {
            if (p.deleteOld) {
              HdfsUtil.deleteHDFSFile(outputPath)
            }
            println("-------------------------in inputDirFlag  --------------")
            val df = sqlContext.read.parquet(inputDir)
            df.registerTempTable("log_data")
            val sqlSelectMedusa = s"select distinct md5(concat(mainCategory,subCategory)) $unique_key,mainCategory,subCategory from log_data "
            sqlContext.sql(sqlSelectMedusa).write.parquet(outputPath)
          }
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}


/*
*

cd /script/bi/medusa/michael/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin
sh /script/bi/medusa/michael/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin/submit.sh \
com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.PlayViewLogDimensionExchange --startDate 20161201






* */
