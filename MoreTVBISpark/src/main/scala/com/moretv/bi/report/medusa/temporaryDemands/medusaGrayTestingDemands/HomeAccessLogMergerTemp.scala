package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Xiajun on 2016/5/9.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/exit
  * input: /mbi/parquet/exit/$date
  * output: /log/medusaAndMoretv/parquet/$date/exit
  */
object HomeAccessLogMergerTemp extends SparkSetting{
   def main(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         val sc = new SparkContext(config)
         val sqlContext = new SQLContext(sc)
         val logType = "homeaccess"
         val cal = Calendar.getInstance()
         val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))



         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
           val logDir1 = s"$medusaDir/$inputDate/$logType"
           val logDir2 = s"$moretvDir/$logType/$inputDate"


           val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",logType)
           val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$logType",inputDate)
           val outputPath = s"/log/medusaAndMoretvMerger/$inputDate/$logType"



           if(medusaFlag && moretvFlag){

             val medusaDf = sqlContext.read.parquet(logDir1)
             val moretvDf = sqlContext.read.parquet(logDir2)
             //       注册临时表
             medusaDf.registerTempTable("log_data_1")
             moretvDf.registerTempTable("log_data_2")

             val sqlSelectMedusa = "select logType, date,datetime,logVersion, event,apkSeries, apkVersion, " +
               "userId,accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime,accessArea," +
               "accessLocation,ip,'' as versionCode,'' as buildDate,locationIndex,'medusa' as flag from log_data_1"
             val sqlSelectMoretv = "select logType, date, datetime, logVersion,'click' as event, apkSeries, apkVersion, " +
               "userId,accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime,accessArea ,accessLocation," +
               "'' as ip,'' as versionCode,'' as buildDate,'' as locationIndex,'moretv' as flag from log_data_2"

             val df1 = sqlContext.sql(sqlSelectMedusa)
             val df2 = sqlContext.sql(sqlSelectMoretv)

             val mergerDf = df1.unionAll(df2)

             mergerDf.write.parquet(outputPath)
           }else if(!medusaFlag && moretvFlag){
             val moretvDf = sqlContext.read.parquet(logDir2)
             moretvDf.registerTempTable("log_data_2")
             val sqlSelectMoretv = "select logType, date, datetime, logVersion,'' as event, apkSeries, apkVersion, " +
               "userId,accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime,accessArea ,accessLocation," +
               "'' as ip,'' as versionCode,'' as buildDate,'' as locationIndex,'moretv' as flag from log_data_2"
             val mergerDf = sqlContext.sql(sqlSelectMoretv)
             mergerDf.write.parquet(outputPath)
           }else if(medusaFlag && !moretvFlag){
             val medusaDf = sqlContext.read.parquet(logDir1)
             medusaDf.registerTempTable("log_data_1")
             val sqlSelectMedusa = "select logType, date,datetime,logVersion, event,apkSeries, apkVersion, " +
               "userId,accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime,accessArea," +
               "accessLocation,ip,'' as versionCode,'' as buildDate,locationIndex,'medusa' as flag from log_data_1"
             val mergerDf = sqlContext.sql(sqlSelectMedusa)
             mergerDf.write.parquet(outputPath)
           }

           cal.add(Calendar.DAY_OF_MONTH, -1)
         })

       }
       case None=>{throw new RuntimeException("At least needs one param: startDate!")}
     }
   }
 }
