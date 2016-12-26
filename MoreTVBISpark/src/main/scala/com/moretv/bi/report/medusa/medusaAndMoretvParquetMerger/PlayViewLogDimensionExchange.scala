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
object PlayViewLogDimensionExchange extends BaseClass {
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
        val inputDirFatTableBase = "/log/medusaAndMoretvMergerDimension"
        val inputLogType = "playview2filter"
        val inputDirDimensionTableBase = "/data_warehouse/dimensions/medusa"
        val inputDimensionType = "sourceList"
        val outputDirBase = "/data_warehouse/medusa"
        val outputType = "play"

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i => {
          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)

          val inputDirFatTable = s"$inputDirFatTableBase/$inputDate/$inputLogType"
          val inputDirDimensionTable = s"$inputDirDimensionTableBase/$inputDate/$inputDimensionType"

          val inputDirFatTableFlag = FilesInHDFS.IsInputGenerateSuccess(inputDirFatTable)
          val inputDirDimensionTableFlag = FilesInHDFS.IsInputGenerateSuccess(inputDirDimensionTable)

          val outputPath = s"$outputDirBase/$inputDate/$outputType"
          println("inputDirFatTable" + inputDirFatTable + ",inputDirDimensionTable:" + inputDirDimensionTable)
          println("inputDirDimensionTableFlag" + inputDirDimensionTableFlag + ",outputPath:" + outputPath)

          if (inputDirFatTableFlag && inputDirDimensionTableFlag) {
            if (p.deleteOld) {
              HdfsUtil.deleteHDFSFile(outputPath)
            }


            if ("" == "") {
              println("-------------------------in inputDirFlag  --------------")
              val df = sqlContext.read.parquet("")
              df.registerTempTable("log_data")
              val sqlSelectMedusa = s"select distinct md5(concat(mainCategory,subCategory)) ,mainCategory,subCategory from log_data "
              sqlContext.sql(sqlSelectMedusa).write.parquet(outputPath)
            }

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
