package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.{PathParser, PathParserDimension, UDFConstantDimension}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by wangbaozhi on 2017/01/05
  * 该类用于过滤单个用户播放当个视频量过大的情况
  */
object PlayViewLogDimensionFilter extends BaseClass {
  private val playNumLimit = 5000

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "4g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "60")
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    println("-------------------------michael test--------------")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val startDate = p.startDate
        val inputDirFatTableBase = UDFConstantDimension.MEDUSA_BIG_FACT_TABLE_DIR
        val inputLogType = UDFConstantDimension.MEDUSA_BIG_FACT_TABLE_PLAY_TYPE
        val outputLogType = UDFConstantDimension.PLAYVIEW

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i => {
          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val inputPath = s"$inputDirFatTableBase/$inputDate/$inputLogType"
          val outputPath = s"$inputDirFatTableBase/$inputDate/$outputLogType"

          val flag = FilesInHDFS.IsDirExist(inputPath)
          if (flag) {
            if (p.deleteOld) {
              HdfsUtil.deleteHDFSFile(outputPath)
            }
            val df = sqlContext.read.parquet(inputPath)
            df.registerTempTable("log")
            val schemaArr = df.columns.toBuffer
            val schemaStr = schemaArr.toList.mkString(",")
            schemaArr += "filterCol"
            schemaArr.toArray
            val arr = df.sqlContext.sql("select userId,videoSid,count(userId) from log group by " +
              "userId,videoSid").map(e => (e.getString(0), e.getString(1), e.getLong(2))).
              filter(_._3 >= playNumLimit).map(e => "'".concat(e._1.concat(e._2)).concat("'")).collect()
            if (arr.length != 0) {
              val filterStr = arr.toList.mkString(",")
              val filterRdd = df.sqlContext.sql(s"select $schemaStr, concat(userId,videoSid) as filterCol from log").
                toDF(schemaArr: _*).filter(s"filterCol not in ($filterStr)")
              filterRdd.write.parquet(outputPath)
            } else {
              val filterRdd = df.sqlContext.sql(s"select $schemaStr,concat(userId,videoSid) as filterCol from log")
              filterRdd.write.parquet(outputPath)
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
