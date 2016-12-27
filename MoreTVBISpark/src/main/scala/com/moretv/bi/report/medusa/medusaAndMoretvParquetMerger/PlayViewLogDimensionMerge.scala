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
  * input: /data_warehouse/dimensions/medusa/daily,/data_warehouse/dimensions/medusa
  * output:/data_warehouse/dimensions/medusa
  *
  * take source_list（列表页分类入口） for example
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
        val inputDirBaseDaily = "/data_warehouse/dimensions/medusa/daily"
        val inputDirBaseOnline = "/data_warehouse/dimensions/medusa"
        val logType = "sourceList"
        val onLineInputDir=s"$inputDirBaseOnline/$logType"
        val onLineOutputDir=s"$inputDirBaseOnline/$logType"
        val unique_key=UDFConstantDimension.SOURCE_LIST_SK

        println("inputDirBaseDaily:"+inputDirBaseDaily+",onLineOutputDir:" + onLineOutputDir)

        //加载历史维度信息 /data_warehouse/dimensions/medusa/daily/20161201/sourceList
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        val inputs = new Array[String](p.numOfDays)
        for(i<- 0 until p.numOfDays){
          val date = dateFormat.format(cal.getTime)
          inputs(i) = s"$inputDirBaseDaily/$date/$logType/"
          cal.add(Calendar.DAY_OF_MONTH,-1)
          println(s"loading $inputDirBaseDaily/$date/$logType/")
        }
        val df_daily=sqlContext.read.parquet(inputs:_*)
        println("df_daily.count():"+df_daily.count())

        //加载生产环境的维度信息
        val isExist=FilesInHDFS.IsInputGenerateSuccess(onLineInputDir)
        if(isExist){
          val df_online=sqlContext.read.parquet(onLineInputDir)
          println("df_online.count():"+df_online.count())
        if (p.deleteOld) {
          HdfsUtil.deleteHDFSFile(onLineOutputDir)
        }

        //做合并，去重，写入hdfs文件夹
        val df_merge=df_daily unionAll df_online
        println("df_merge.count():"+df_merge.count())

        val df_result=df_merge.dropDuplicates(Array(unique_key))
        println("df_result.count():"+df_result.count())
        df_result.write.parquet(onLineOutputDir)
        }else{
          if (p.deleteOld) {
            HdfsUtil.deleteHDFSFile(onLineOutputDir)
          }
          df_daily.write.parquet(onLineOutputDir)
        }
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
