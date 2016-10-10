package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.util.Calendar

import com.moretv.bi.report.medusa.util.udf.PathParser
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
object PlayViewLogMerger extends SparkSetting{
   def main(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         val sc = new SparkContext(config)
         val sqlContext = new SQLContext(sc)
         sqlContext.udf.register("pathParser",PathParser.pathParser _)
         val startDate = p.startDate
         val logType1 = "playview"
         val logType2 = "playview"
         val appMode1 = "medusa"
         val appMode2 = "moretv"
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
           val logDir1 = s"/log/medusa/parquet/$inputDate/$logType1"
           val logDir2 = s"/mbi/parquet/$logType2/$inputDate"
           val medusaDf = sqlContext.read.parquet(logDir1)
           val moretvDf = sqlContext.read.parquet(logDir2)
           val outputPath = s"/log/temp/medusaAndMoretvMerger/parquet/$inputDate/$logType2"

           //       注册临时表
           medusaDf.registerTempTable("log_data_1")
           moretvDf.registerTempTable("log_data_2")

           val sqlSelectMedusa = "select logType, date, datetime, logVersion, event, apkSeries, apkVersion, userId,accountId," +
             "groupId, promotionChannel, weatherCode, productModel, uploadTime, duration,contentType,videoSid,episodeSid," +
             "retrieval,searchText,ip,versionCode,buildDate,pathMain,pathSub,pathSpecial,'' as path, " +
             s"pathParser('play',pathMain,'pathMain','launcherArea') as launcherAreaFromPath, " +
             s"pathParser('play',pathMain,'pathMain','launcherAccessLocation') as launcherAccessLocationFromPath, " +
             s"pathParser('play',pathMain,'pathMain','pageType') as pageTypeFromPath, " +
             s"pathParser('play',pathMain,'pathMain','pageDetailInfo') as pageDetailInfoFromPath, " +
             s"pathParser('play',pathSub,'pathSub','accessPath') as accessPathFromPath, " +
             s"pathParser('play',pathSub,'pathSub','previousSid') as previousSidFromPath, " +
             s"pathParser('play',pathSub,'pathSub','previousContentType') as previousContentTypeFromPath, " +
             s"pathParser('play',pathSpecial,'pathSpecial','pathProperty') as pathPropertyFromPath, " +
             s"pathParser('play',pathSpecial,'pathSpecial','pathIdentification') as pathIdentificationFromPath," +
             s"'medusa' as flag " +
             " from log_data_1"
           val sqlSelectMoretv = "select logType, date, datetime, logVersion, event, apkSeries, apkVersion, userId,accountId," +
             "groupId, promotionChannel, weatherCode, productModel, uploadTime, duration,contentType,videoSid,episodeSid," +
             "'' as retrieval,'' as searchText,'' as ip,'' as versionCode,'' as buildDate,'' as pathMain,'' as pathSub,'' as " +
             "pathSpecial," +
             "path," +
             s"pathParser('playview',path,'path','launcherArea') as launcherAreaFromPath, " +
             s"pathParser('playview',path,'path','launcherAccessLocation') as launcherAccessLocationFromPath, " +
             s"pathParser('playview',path,'path','pageType') as pageTypeFromPath, " +
             s"pathParser('playview',path,'path','pageDetailInfo') as pageDetailInfoFromPath, " +
             s"pathParser('playview',path,'path','accessPath') as accessPathFromPath, " +
             s"pathParser('playview',path,'path','previousSid') as previousSidFromPath, " +
             s"pathParser('playview',path,'path','previousContentType') as previousContentTypeFromPath, " +
             s"pathParser('playview',path,'path','pathProperty') as pathPropertyFromPath, " +
             s"pathParser('playview',path,'path','pathIdentification') as pathIdentificationFromPath,'moretv' as flag " +
             " from log_data_2"

           val df1 = sqlContext.sql(sqlSelectMedusa)
           val df2 = sqlContext.sql(sqlSelectMoretv)

           val mergerDf = df1.unionAll(df2)


           mergerDf.write.parquet(outputPath)
           cal.add(Calendar.DAY_OF_MONTH, -1)
         })


       }
       case None=>{throw new RuntimeException("At least needs one param: startDate!")}
     }
   }
 }
