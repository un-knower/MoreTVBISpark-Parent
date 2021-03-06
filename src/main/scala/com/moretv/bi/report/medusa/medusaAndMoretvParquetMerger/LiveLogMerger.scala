package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.TimeParser
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by Xiajun on 2016/5/9.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/detail
  * input: /mbi/parquet/detail/$date
  * output: /log/medusaAndMoretv/parquet/$date/detail
  */
object LiveLogMerger extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
   override def execute(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         sqlContext.udf.register("getHourFromDateTime",TimeParser.getHourFromDateTime _)
         sqlContext.udf.register("getChannelName",LiveCodeToNameUtils.getChannelNameBySid _)
         /*val logType = "live"
         val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"*/
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          /* val logDir1 = s"$medusaDir/$inputDate/$logType"
           val logDir2 = s"$moretvDir/$logType/$inputDate"*/


           // 判断文件是否存在
          /* val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",logType)
           val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$logType",inputDate)*/

           val medusa_input_dir= DataIO.getDataFrameOps.getPath(MEDUSA,LogTypes.LIVE,inputDate)
           val moretv_input_dir= DataIO.getDataFrameOps.getPath(MORETV,LogTypes.LIVE,inputDate)
           val outputPath= DataIO.getDataFrameOps.getPath(MERGER,LogTypes.LIVE,inputDate)
           val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
           val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)

           //val outputPath = s"/log/medusaAndMoretvMerger/$inputDate/$logType"
           if(medusaFlag && moretvFlag){
             //val medusaDf = sqlContext.read.parquet(logDir1).repartition(16).persist(StorageLevel.DISK_ONLY)
             //val moretvDf = sqlContext.read.parquet(logDir2).repartition(16).persist(StorageLevel.DISK_ONLY)
             val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.LIVE,inputDate).persist(StorageLevel.DISK_ONLY)
             val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.LIVE,inputDate).persist(StorageLevel.DISK_ONLY)


             val medusaColNames = medusaDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
             val moretvColNames = moretvDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
             medusaDf.registerTempTable("log_data_1")
             moretvDf.registerTempTable("log_data_2")
             var sqlSelectMedusa = ""
             var sqlSelectMoretv = ""
              if(!medusaColNames.contains("liveName")){
                sqlSelectMedusa = s"select $medusaColNames, getChannelName(channelSid) as liveName, " +
                  s"getHourFromDateTime" +
                  s"(datetime) as hour,'medusa' as flag from log_data_1"
              } else{
                sqlSelectMedusa = s"select $medusaColNames,case when liveName is null then getChannelName" +
                  s"(channelSid) else" +
                  s" liveName end as liveName,getHourFromDateTime(datetime) as hour,'medusa' as flag from log_data_1"
              }
              sqlSelectMoretv = s"select $moretvColNames,date as day, getChannelName(channelSid) as liveName," +
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
             //val moretvDf = sqlContext.read.parquet(logDir2)
             val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.LIVE,inputDate)

             val moretvColNames = moretvDf.columns.toList.mkString(",")
             moretvDf.registerTempTable("log_data_2")
             val sqlSelectMoretv = s"select $moretvColNames,date as day, getChannelName(channelSid) as liveName," +
               s"getHourFromDateTime(datetime) as hour,'moretv' as flag " +
               s"from log_data_2"
             sqlContext.sql(sqlSelectMoretv).write.parquet(outputPath)
           }else if(medusaFlag && !moretvFlag){
             //val medusaDf = sqlContext.read.parquet(logDir1)
             val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.LIVE,inputDate)

             val medusaColNames = medusaDf.columns.toList.mkString(",")
             medusaDf.registerTempTable("log_data_1")
             var sqlSelectMedusa = ""
             if(!medusaColNames.contains("liveName")){
               sqlSelectMedusa = s"select $medusaColNames,getChannelName(channelSid) as liveName,getHourFromDateTime" +
                 s"(datetime) as hour,'medusa' as flag from log_data_1"
             } else{
               sqlSelectMedusa = s"select $medusaColNames,case when liveName is null then getChannelName(channelSid) else" +
                 s" liveName end as liveName,getHourFromDateTime(datetime) as hour,'medusa' as flag from log_data_1"
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
