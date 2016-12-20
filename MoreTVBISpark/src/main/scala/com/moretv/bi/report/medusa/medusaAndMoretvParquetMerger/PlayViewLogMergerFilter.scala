package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar

import com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.DetailLogMerger._
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.PathParser
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
  * Created by Xiajun on 2016/5/9.
  * 该类用于过滤单个用户播放当个视频量过大的情况
  */
object PlayViewLogMergerFilter extends BaseClass{
  private val playNumLimit = 5000
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(PlayViewLogMergerFilter,args)
  }

   override def execute(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
         val outLogType = "playview"
         val inputLogType = "playview2filter"
         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
           val inputDir = s"/log/medusaAndMoretvMerger/$inputDate/$inputLogType"
           val outputPath = s"/log/medusaAndMoretvMerger/$inputDate/$outLogType"
           if(p.deleteOld){
             HdfsUtil.deleteHDFSFile(outputPath)
           }
           val df = sqlContext.read.load(inputDir)
           df.registerTempTable("log")
           val schemaArr = df.columns.toBuffer
           val schemaStr = schemaArr.toList.mkString(",")
           schemaArr += "filterCol"
           schemaArr.toArray
           val arr = sqlContext.sql("select userId,videoSid,count(userId) from log group by " +
             "userId,videoSid").map(e=>(e.getString(0),e.getString(1),e.getLong(2))).
             filter(_._3>=playNumLimit).map(e=>"'".concat(e._1.concat(e._2)).concat("'")).collect()
           if(arr.length!=0){
             val filterStr = arr.toList.mkString(",")
             val filterRdd = sqlContext.sql(s"select $schemaStr, concat(userId,videoSid) as filterCol from log").
               toDF(schemaArr:_*).filter(s"filterCol not in ($filterStr)")
             filterRdd.write.parquet(outputPath)
           }else{
             val filterRdd = sqlContext.sql(s"select $schemaStr,concat(userId,videoSid) as filterCol from log")
             filterRdd.write.parquet(outputPath)
           }
           cal.add(Calendar.DAY_OF_MONTH, -1)
         })


       }
       case None=>{throw new RuntimeException("At least needs one param: startDate!")}
     }
   }
 }
