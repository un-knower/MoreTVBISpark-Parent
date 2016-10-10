package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.InfoTransform
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Xiajun on 2016/5/9.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/detail
  * input: /mbi/parquet/detail/$date
  * output: /log/medusaAndMoretv/parquet/$date/detail
  */
object EvaluateLogMergerTemp extends SparkSetting{
   def main(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         val sc = new SparkContext(config)
         val sqlContext = new SQLContext(sc)
         sqlContext.udf.register("transformEventInEvaluate",InfoTransform.transformEventInEvaluate _)
//         sqlContext.udf.register("getProgramName",CodeToNameUtils.getProgramNameBySid _)
         val medusaLogType = "evaluate"
         val moretvLogType = "operation-e"
         val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
           val logDir1 = s"$medusaDir/$inputDate/$medusaLogType"
           val logDir2 = s"$moretvDir/$moretvLogType/$inputDate"


           val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",medusaLogType)
           val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$moretvLogType",inputDate)

           val outputPath = s"/log/medusaAndMoretvMerger/$inputDate/$medusaLogType"

           if(medusaFlag && moretvFlag){
             val medusaDf = sqlContext.read.parquet(logDir1)
             val moretvDf = sqlContext.read.parquet(logDir2)

             //       注册临时表
             medusaDf.registerTempTable("log_data_1")
             moretvDf.registerTempTable("log_data_2")

             val sqlSelectMedusa = "select logType, date,datetime, logVersion, event, apkSeries, apkVersion, userId," +
               "accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime, ip,videoSid,contentType," +
               "'' as versionCode,'' as buildDate,'medusa' as flag from log_data_1"
             val sqlSelectMoretv = "select logType, date, datetime, logVersion, transformEventInEvaluate(action) as event," +
               " apkSeries, apkVersion,userId,accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime," +
               "' ' as ip,videoSid,contentType,'' as versionCode,'' as buildDate," +
               " 'moretv' as flag from log_data_2"

             val df1 = sqlContext.sql(sqlSelectMedusa)
             val df2 = sqlContext.sql(sqlSelectMoretv)

             val mergerDf = df1.unionAll(df2)
             mergerDf.write.parquet(outputPath)

           }else if(!medusaFlag && moretvFlag){
             val moretvDf = sqlContext.read.parquet(logDir2)
             moretvDf.registerTempTable("log_data_2")

             val sqlSelectMoretv = "select logType, date, datetime, logVersion, transformEventInEvaluate(action) as event," +
               " apkSeries, apkVersion,userId,accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime,'' as ip, videoSid," +
               "contentType,'' as versionCode,'' as buildDate,'moretv' as flag from log_data_2"
             val mergerDf = sqlContext.sql(sqlSelectMoretv)
             mergerDf.write.parquet(outputPath)
           }else if(medusaFlag && !moretvFlag){
             val medusaDf = sqlContext.read.parquet(logDir1)
             medusaDf.registerTempTable("log_data_1")
             val sqlSelectMedusa = "select logType, date, datetime, logVersion, event, apkSeries, apkVersion, userId," +
               "accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime, ip,videoSid,contentType," +
               "'' as versionCode,'' as buildDate,'medusa' as flag  from log_data_1"
             val mergerDf = sqlContext.sql(sqlSelectMedusa)
             mergerDf.write.parquet(outputPath)
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
