package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar

import com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger.DetailLogMerger._
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.PathParser
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{CodeToNameUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
  * Created by Xiajun on 2016/5/9.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/playview
  * input: /mbi/parquet/playview/$date
  * output: /log/medusaAndMoretv/parquet/$date/playview2
  */
object PlayViewLogMergerV2 extends BaseClass{

  def main (args: Array[String]) {
    ModuleClass.executor(this,args)
  }
   override def execute(args: Array[String]) {
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         sqlContext.udf.register("pathParser",PathParser.pathParser _)
//         sqlContext.udf.register("getProgramName",CodeToNameUtils.getProgramNameBySid _)
         val startDate = p.startDate
         /*val medusaLogType = "playview"
         val moretvLogType = "playview"
         val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"
         val outputLogType="playview2"*/
         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          /* val logDir1 = s"/log/medusa/parquet/$inputDate/$medusaLogType"
           val logDir2 = s"/mbi/parquet/$moretvLogType/$inputDate"

           val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",medusaLogType)
           val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$moretvLogType",inputDate)
           val outputPath = s"/log/medusaAndMoretvMerger/$inputDate/$outputLogType"*/


           val medusa_input_dir= DataIO.getDataFrameOps.getPath(MEDUSA,LogTypes.PLAYVIEW,inputDate)
           val moretv_input_dir= DataIO.getDataFrameOps.getPath(MORETV,LogTypes.PLAYVIEW,inputDate)
           val outputPath= DataIO.getDataFrameOps.getPath(MERGER,LogTypes.PLAY_VIEW_2,inputDate)
           val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
           val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)


           if(medusaFlag && moretvFlag){
          /*   val medusaDf = sqlContext.read.parquet(logDir1)
             val moretvDf = sqlContext.read.parquet(logDir2)*/
             val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.PLAYVIEW,inputDate)
             val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.PLAYVIEW,inputDate)


             //       注册临时表
             medusaDf.registerTempTable("log_data_1")
             moretvDf.registerTempTable("log_data_2")

             val sqlSelectMedusa = "select logType, date,datetime, logVersion, event, apkSeries, apkVersion, userId," +
               "accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime, duration,contentType,videoSid," +
               "episodeSid,retrieval,searchText,ip,versionCode,buildDate,pathMain,pathSub,pathSpecial,'' as path, " +
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
               s"pathParser('playview',path,'path','pathIdentification') as pathIdentificationFromPath," +
               s"'moretv' as flag " +
               " from log_data_2"

             val df1 = sqlContext.sql(sqlSelectMedusa)
             val df2 = sqlContext.sql(sqlSelectMoretv)

             val mergerDf = df1.unionAll(df2)


             mergerDf.write.parquet(outputPath)
           }else if(!medusaFlag && moretvFlag){
             //val moretvDf = sqlContext.read.parquet(logDir2)
             val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.PLAYVIEW,inputDate)


             moretvDf.registerTempTable("log_data_2")
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
               s"pathParser('playview',path,'path','pathIdentification') as pathIdentificationFromPath, " +
               s"'moretv' as flag " +
               " from log_data_2"

             val mergerDf=sqlContext.sql(sqlSelectMoretv)
             mergerDf.write.parquet(outputPath)

           }else if(medusaFlag && !moretvFlag){
             //val medusaDf = sqlContext.read.parquet(logDir1)
             val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.PLAYVIEW,inputDate)
             medusaDf.registerTempTable("log_data_1")
             val sqlSelectMedusa = "select logType, date,datetime, logVersion, event, apkSeries, apkVersion, " +
               "userId,accountId," +
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
