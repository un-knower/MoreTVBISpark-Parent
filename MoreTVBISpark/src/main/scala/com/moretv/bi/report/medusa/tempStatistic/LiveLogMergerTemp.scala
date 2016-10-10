package com.moretv.bi.report.medusa.tempStatistic

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.TimeParser
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Xiajun on 2016/5/9.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/detail
  * input: /mbi/parquet/detail/$date
  * output: /log/medusaAndMoretv/parquet/$date/detail
  */
object LiveLogMergerTemp extends BaseClass{
  def main(args: Array[String]) {
    config.set("spark.eventLog.enabled","true").
      set("spark.eventLog.dir","hdfs://hans/xiajun/spark-events").
      set("spark.speculation","true").
      set("spark.speculation.interval", "100").
      set("spark.speculation.quantile","0.75").
      set("spark.speculation.multiplier","1.5")
    ModuleClass.executor(LiveLogMergerTemp,args)
  }
   override def execute(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         sqlContext.udf.register("getHourFromDateTime",TimeParser.getHourFromDateTime _)
         val logType = "live"
         val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
           val logDir1 = s"$medusaDir/$inputDate/$logType"
           val logDir2 = s"$moretvDir/$logType/$inputDate"


           // 判断文件是否存在
           val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",logType)
           val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$logType",inputDate)

           val outputPath = s"/log/medusaAndMoretvMerger/$inputDate/$logType"
           if(medusaFlag && moretvFlag){
             val medusaDf = sqlContext.read.parquet(logDir1).repartition(16).persist(StorageLevel.DISK_ONLY)
             val moretvDf = sqlContext.read.parquet(logDir2).repartition(16).persist(StorageLevel.DISK_ONLY)
             val medusaColNames = medusaDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
             val moretvColNames = moretvDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
             medusaDf.registerTempTable("log_data_1")
             moretvDf.registerTempTable("log_data_2")
             var sqlSelectMedusa = ""
             var sqlSelectMoretv = ""
              if(!medusaColNames.contains("liveName")){
                sqlSelectMedusa = s"select $medusaColNames, " +
                  s"getHourFromDateTime" +
                  s"(datetime) as hour,'medusa' as flag from log_data_1"
              } else{
                sqlSelectMedusa = s"select $medusaColNames,getHourFromDateTime(datetime) as hour,'medusa' as flag from log_data_1"
              }
              sqlSelectMoretv = s"select $moretvColNames,date as day," +
                s"getHourFromDateTime(datetime) as hour,'moretv' as flag from log_data_2"

             val rdd1 = sqlContext.sql(sqlSelectMedusa).toJSON
             val rdd2 = sqlContext.sql(sqlSelectMoretv).toJSON
             val rdd = rdd1.union(rdd2)
             if(p.deleteOld) {
               HdfsUtil.deleteHDFSFile(outputPath)
             }
               sqlContext.read.json(rdd).write.parquet(outputPath)
             medusaDf.unpersist()
             moretvDf.unpersist()
           }else if(!medusaFlag && moretvFlag){
             val moretvDf = sqlContext.read.parquet(logDir2)
             val moretvColNames = moretvDf.columns.toList.mkString(",")
             moretvDf.registerTempTable("log_data_2")
             val sqlSelectMoretv = s"select $moretvColNames,date as day, " +
               s"getHourFromDateTime(datetime) as hour,'moretv' as flag " +
               s"from log_data_2"
             sqlContext.sql(sqlSelectMoretv).write.parquet(outputPath)
           }else if(medusaFlag && !moretvFlag){
             val medusaDf = sqlContext.read.parquet(logDir1)
             val medusaColNames = medusaDf.columns.toList.mkString(",")
             medusaDf.registerTempTable("log_data_1")
             var sqlSelectMedusa = ""
             if(!medusaColNames.contains("liveName")){
               sqlSelectMedusa = s"select $medusaColNames,getHourFromDateTime" +
                 s"(datetime) as hour,'medusa' as flag from log_data_1"
             } else{
               sqlSelectMedusa = s"select $medusaColNames,getHourFromDateTime(datetime) as hour,'medusa' as flag from log_data_1"
             }
             sqlContext.sql(sqlSelectMedusa).write.parquet(outputPath)
           }else{
             throw new RuntimeException("Do not exists files!")
           }
           cal.add(Calendar.DAY_OF_MONTH, -1)
         })

       }
       case None=>{throw new RuntimeException("At least needs one param: startDate!")}
     }
   }
 }
