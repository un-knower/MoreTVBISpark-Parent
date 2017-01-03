package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.UDFConstantDimension
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by wangbaozhi on 2016/12/26,
  * This object is used to merge dimension of daily and online dimension,generate the new online dimension
  * input: /data_warehouse/dw_dimensions/daily/20161201/dim_medusa_source_retrieval/
  *        /data_warehouse/dw_dimensions/dim_medusa_source_retrieval
  * output:/data_warehouse/dw_dimensions/dim_medusa_source_retrieval
  *
  */
object PlayViewLogDimensionMerge extends BaseClass {
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    println("-------------------------PlayViewLogDimensionMerge test--------------")
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val startDate = p.startDate
        val inputDirBaseDaily = UDFConstantDimension.MEDUSA_DAILY_DIMENSION_DATA_WAREHOUSE
        val inputDirBaseOnline = UDFConstantDimension.MEDUSA_DIMENSION_DATA_WAREHOUSE
        var logTypeAndUniqueKeyMap: Map[String, String] = Map()
        logTypeAndUniqueKeyMap += (UDFConstantDimension.SOURCE_RETRIEVAL_TABLE -> UDFConstantDimension.SOURCE_RETRIEVAL_SK)
        logTypeAndUniqueKeyMap += (UDFConstantDimension.SOURCE_SEARCH_TABLE -> UDFConstantDimension.SOURCE_SEARCH_SK)
        logTypeAndUniqueKeyMap += (UDFConstantDimension.SOURCE_LIST_TABLE -> UDFConstantDimension.SOURCE_LIST_SK)
        logTypeAndUniqueKeyMap += (UDFConstantDimension.SOURCE_RECOMMEND_TABLE -> UDFConstantDimension.SOURCE_RECOMMEND_SK)
        logTypeAndUniqueKeyMap += (UDFConstantDimension.SOURCE_SPECIAL_TABLE -> UDFConstantDimension.SOURCE_SPECIAL_SK)
        logTypeAndUniqueKeyMap += (UDFConstantDimension.SOURCE_LAUNCHER_TABLE -> UDFConstantDimension.SOURCE_LAUNCHER_SK)

        logTypeAndUniqueKeyMap.keys.foreach { logType =>
          print("logType = " + logType)
          println(" UniqueKey = " + logTypeAndUniqueKeyMap(logType))

          val onLineDimensionDir = s"$inputDirBaseOnline/$logType"
          val unique_key = logTypeAndUniqueKeyMap(logType)
          val onLineDimensionDirTmp=s"${onLineDimensionDir}_tmp"
          println("inputDirBaseDaily:" + inputDirBaseDaily)
          println("onLineDimensionDir:" + onLineDimensionDir)
          println("onLineDimensionDirTmp:" + onLineDimensionDirTmp)

          //加载历史维度信息 /data_warehouse/dw_dimensions/daily/20161201/dim_medusa_source_retrieval/
          val cal = Calendar.getInstance()
          cal.setTime(DateFormatUtils.readFormat.parse(startDate))
          val inputs = new Array[String](p.numOfDays)
          for (i <- 0 until p.numOfDays) {
            val date = dateFormat.format(cal.getTime)
            inputs(i) = s"$inputDirBaseDaily/$date/$logType/"
            cal.add(Calendar.DAY_OF_MONTH, -1)
            println(s"loading $inputDirBaseDaily/$date/$logType/")
          }
          val df_daily = sqlContext.read.parquet(inputs: _*)
          println("每日维度数量:" + df_daily.count())
          val distinct_df_daily=df_daily.dropDuplicates(Array(unique_key))
          println("每日维度去重后维度数量:" + distinct_df_daily.count())

          //加载生产环境的维度信息
          val isExist = FilesInHDFS.IsInputGenerateSuccess(onLineDimensionDir)
          if (isExist) {
            println("====合并每日的维度和线上维度，去重，写入hdfs文件夹")
            val df_online = sqlContext.read.parquet(onLineDimensionDir)
            println("线上维度数量:" + df_online.count())
            val df_merge = distinct_df_daily unionAll df_online
            println("合并后维度数量:" + df_merge.count())

            val df_result = df_merge.dropDuplicates(Array(unique_key))
            println("合并去重后维度数量:" + df_result.count())
            HdfsUtil.deleteHDFSFile(onLineDimensionDirTmp)
            df_result.write.parquet(onLineDimensionDirTmp)
            if (p.deleteOld) {
              HdfsUtil.deleteHDFSFile(onLineDimensionDir)
            }
            val isSuccess=HdfsUtil.copyFilesInDir(onLineDimensionDirTmp,onLineDimensionDir)
            println("is copy success:"+isSuccess)
          } else {
            val isDirExist= FilesInHDFS.IsDirExist(onLineDimensionDir)
            if(isDirExist){
              HdfsUtil.deleteHDFSFile(onLineDimensionDir)
            }
              println("====完全使用每天的维度表信息生成线上维度表信息")
              distinct_df_daily.write.parquet(onLineDimensionDir)
          }
        }
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
