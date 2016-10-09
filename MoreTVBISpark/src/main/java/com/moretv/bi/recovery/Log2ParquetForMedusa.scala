package com.moretv.bi.recovery

import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
 * Created by zhangyu on 2015/9/1.
 * 过滤parquet日志中脏字段列名,将现有parquet日志中的脏字段列及对应的数据剔除,而后暂时存入一个临时目录.
 * 这里的脏字段列是指包含了非数字和字母的.
 */
object Log2ParquetForMedusa extends BaseClass{

  private val outputPartitionNum = 40

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.cores.max", "30").
      set("spark.executor.cores", "5").
      set("spark.storage.memoryFraction", "0.1")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val regexWord = "^\\w+$".r
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val start = System.currentTimeMillis()

          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          //val inputPath = s"/log/medusa/rawlog/$inputDate/*"
          val outputPath = s"/log/medusa/parquet/$inputDate/"
          val logType = "play"
          val tmpPath = s"/log/medusa/tmp/$inputDate/"

          val df = sqlContext.read.parquet(outputPath + logType)
          val columns = df.columns
          val realColumns = columns.map(column => {
            regexWord findFirstIn column match {
              case Some(m) => column
              case None => null
            }
          }).filter(_ != null)
         // val columnStr = realColumns.mkString(",")
          val realData = df.select(realColumns.head,realColumns.tail:_*)
          realData.write.parquet(tmpPath + logType)
          val end = System.currentTimeMillis()
          println((end-start)/1000)


          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

}
