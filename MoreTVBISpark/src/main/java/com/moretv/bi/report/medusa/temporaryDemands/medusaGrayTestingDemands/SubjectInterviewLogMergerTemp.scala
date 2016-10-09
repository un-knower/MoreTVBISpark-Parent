package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.util.{CodeToNameUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
  * Created by Xiajun on 2016/5/9.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/subject
  * input: /mbi/parquet/interview/$date
  * output: /log/medusaAndMoretv/parquet/$date/subject_interview
  */
object SubjectInterviewLogMergerTemp extends SparkSetting{
   def main(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         val sc = new SparkContext(config)
         val sqlContext = new SQLContext(sc)
         sqlContext.udf.register("getSubjectName",CodeToNameUtils.getSubChannelNameByCode _)
         val startDate = p.startDate
         val medusaLogType = "subject"
         val moretvLogType = "detail-subject"
         val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"
         val outputLogType = "subject-interview"
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
           val logDir1 = s"/log/medusa/parquet/$inputDate/$medusaLogType"
           val logDir2 = s"/mbi/parquet/$moretvLogType/$inputDate"

           val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",medusaLogType)
           val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$moretvLogType",inputDate)
           val outputPath = s"/log/medusaAndMoretvMerger/$inputDate/$outputLogType"


           if(medusaFlag && moretvFlag){
             val medusaDf = sqlContext.read.parquet(logDir1)
             val moretvDf = sqlContext.read.parquet(logDir2)
             //       注册临时表
             medusaDf.registerTempTable("log_data_1")
             moretvDf.registerTempTable("log_data_2")

             val sqlSelectMedusa = "select logType, date,datetime, logVersion, event, apkSeries, apkVersion, userId," +
               "accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime, duration," +
               "subjectCode,ip,'' as versionCode,'' as  buildDate,'' as path,getSubjectName(subjectCode) as subjectTitle," +
               "'medusa' as " +
               "flag " +
               " from log_data_1"
             val sqlSelectMoretv = "select logType, date, datetime, logVersion, event, apkSeries, apkVersion, userId," +
               "accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime, '' as duration,subjectCode,'' " +
               "as ip" +
               ",'' as versionCode,'' as buildDate,path,getSubjectName(subjectCode) as subjectTitle,'moretv' as flag " +
               " from log_data_2"

             val df1 = sqlContext.sql(sqlSelectMedusa)
             val df2 = sqlContext.sql(sqlSelectMoretv)

             val mergerDf = df1.unionAll(df2)


             mergerDf.write.parquet(outputPath)
           }else if(!medusaFlag && moretvFlag){
             val moretvDf = sqlContext.read.parquet(logDir2)
             moretvDf.registerTempTable("log_data_2")
             val sqlSelectMoretv = "select logType, date, datetime, logVersion, event, apkSeries, apkVersion, userId," +
               "accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime, duration, getSubjectCode(path)" +
               " as subjectCode,'' as ip,'' as versionCode,'' as buildDate,path,getSubjectName(subjectCode) as subjectTitle,'moretv' as flag " +
               " from log_data_2"

             val mergerDf=sqlContext.sql(sqlSelectMoretv)
             mergerDf.write.parquet(outputPath)

           }else if(medusaFlag && !moretvFlag){
             val medusaDf = sqlContext.read.parquet(logDir1)
             medusaDf.registerTempTable("log_data_1")
             val sqlSelectMedusa = "select logType, date,datetime, logVersion, event, apkSeries, apkVersion, userId," +
               "accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime, duration," +
               "subjectCode,ip,'' as  versionCode,'' as buildDate,'' as path,getSubjectName(subjectCode) as subjectTitle," +
               "'medusa' as flag " +
               " from log_data_1"

             val mergerDf=sqlContext.sql(sqlSelectMedusa)
             mergerDf.write.parquet(outputPath)
           }


           cal.add(Calendar.DAY_OF_MONTH, -1)
         })


       }
       case None=>{throw new RuntimeException("At least needs one param: startDate!")}
     }
   }
 }
