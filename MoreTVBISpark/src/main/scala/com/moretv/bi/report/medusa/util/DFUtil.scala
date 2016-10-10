package com.moretv.bi.report.medusa.util

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util.SparkSetting
import org.apache.spark.sql.SQLContext
import tachyon.TachyonURI
import tachyon.client.TachyonFS

/**
 * Created by Xiajun on 2016/5/3.
 */
object DFUtil extends SparkSetting{

  private val dateFormat = new SimpleDateFormat("yyyyMMdd")

  /*此函数用户获取parquet中的数据，并将其转换为DataFrame*/
  def getOriginalDFByDate(logType:String,startDate:String,fileDir:String,
                          applicationMode:String,numOfDays:Int=1,mode:String="hdfs")(implicit sqlContext:SQLContext)={
    /*require用于限制程序运行时，必须满足的条件*/
    require(Set("tachyon","hdfs").contains(mode))
    require(fileDir!=null)
    val calendar = Calendar.getInstance()
    calendar.setTime(dateFormat.parse(startDate))
    val inputs = new Array[String](numOfDays)

    mode match {
      case "tachyon" =>
        val tfs = TachyonFS.get(new TachyonURI(tachyonMaster))
        for(i<- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          val tachyonDir = s"$tachyonMaster/tachyon/$fileDir/$logType/$date/"
          if(tfs.exist(new TachyonURI(tachyonDir))){
            inputs(i) = tachyonDir
          }else{
            if(applicationMode=="motetv"){
              inputs(i) = s"$fileDir/$logType/$date/"
            }else if(applicationMode=="medusa"){
              inputs(i) = s"$fileDir/$date/$logType/"
            }

          }
          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }

      case "hdfs" =>
        for(i<- 0 until numOfDays){
          val date = dateFormat.format(calendar.getTime)
          if(applicationMode=="medusa"){
            inputs(i) = s"$fileDir/$date/$logType/"
          }else if(applicationMode=="moretv"){
            inputs(i) = s"$fileDir/$logType/$date/"
          }

          calendar.add(Calendar.DAY_OF_MONTH,-1)
        }
    }
    sqlContext.read.parquet(inputs:_*)
  }
}
