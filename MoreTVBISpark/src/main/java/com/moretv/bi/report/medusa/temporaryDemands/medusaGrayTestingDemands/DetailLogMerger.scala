package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.PathParser
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * Created by Xiajun on 2016/5/9.
 * This object is used to merge the parquet data of medusa and moretv into one parquet!
 * input: /log/medusa/parquet/$date/detail
 * input: /mbi/parquet/detail/$date
 * output: /log/medusaAndMoretv/parquet/$date/detail
 */
object DetailLogMerger extends BaseClass{
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(DetailLogMerger,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p)=>{

        sqlContext.udf.register("pathParser",PathParser.pathParser _)
//        sqlContext.udf.register("getProgramName",ProgramRedisUtil.getTitleBySid _)
        val startDate = p.startDate
        val logType = "detail"
        val medusaLogType = "detail"
        val moretvLogType = "detail"
        val medusaDir = "/log/medusa/parquet"
        val moretvDir = "/mbi/parquet"
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val logDir1 = s"$medusaDir/$inputDate/$logType"
          val logDir2 = s"$moretvDir/$logType/$inputDate"


          val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",medusaLogType)
          val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$moretvLogType",inputDate)
          val outputPath = s"/log/medusa/temp/merger/$inputDate/$logType"


          if(medusaFlag && moretvFlag){
            val medusaDf = sqlContext.read.parquet(logDir1)
            val moretvDf = sqlContext.read.parquet(logDir2)
            //       注册临时表
            medusaDf.registerTempTable("log_data_1")
            moretvDf.registerTempTable("log_data_2")

            val sqlSelectMedusa = s"select logType, date,datetime,logVersion, event, apkSeries, apkVersion, " +
              s"userId,accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime,contentType,videoSid,ip," +
              s"retrieval,searchText,'' as versionCode,'' as buildDate,pathMain,pathSub,pathSpecial,'' as path, " +
              s"pathParser('detail',pathMain,'pathMain','launcherArea') as launcherAreaFromPath, " +
              s"pathParser('detail',pathMain,'pathMain','launcherAccessLocation') as launcherAccessLocationFromPath, " +
              s"pathParser('detail',pathMain,'pathMain','pageType') as pageTypeFromPath, " +
              s"pathParser('detail',pathMain,'pathMain','pageDetailInfo') as pageDetailInfoFromPath, " +
              s"pathParser('detail',pathSub,'pathSub','accessPath') as accessPathFromPath, " +
              s"pathParser('detail',pathSub,'pathSub','previousSid') as previousSidFromPath, " +
              s"pathParser('detail',pathSub,'pathSub','previousContentType') as previousContentTypeFromPath, " +
              s"pathParser('detail',pathSpecial,'pathSpecial','pathProperty') as pathPropertyFromPath, " +
              s"pathParser('detail',pathSpecial,'pathSpecial','pathIdentification') as pathIdentificationFromPath," +
              s"'medusa' as flag " +
              s"from log_data_1"
            val sqlSelectMoretv = s"select logType, date, datetime, logVersion, event, apkSeries, apkVersion, userId," +
              s"accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime,contentType,videoSid,'' as ip,'' " +
              s"as retrieval,'' as searchText,'' as versionCode,'' as buildDate,'' as pathMain,'' as pathSub, '' as pathSpecial,path, "+
              s"pathParser('detail',path,'path','launcherArea') as launcherAreaFromPath, " +
              s"pathParser('detail',path,'path','launcherAccessLocation') as launcherAccessLocationFromPath, " +
              s"pathParser('detail',path,'path','pageType') as pageTypeFromPath, " +
              s"pathParser('detail',path,'path','pageDetailInfo') as pageDetailInfoFromPath, " +
              s"pathParser('detail',path,'path','accessPath') as accessPathFromPath, " +
              s"pathParser('detail',path,'path','previousSid') as previousSidFromPath, " +
              s"pathParser('detail',path,'path','previousContentType') as previousContentTypeFromPath, " +
              s"pathParser('detail',path,'path','pathProperty') as pathPropertyFromPath, " +
              s"pathParser('detail',path,'path','pathIdentification') as pathIdentificationFromPath," +
              s"'moretv' as flag " +
              s" from log_data_2"

            val df1 = sqlContext.sql(sqlSelectMedusa)
            val df2 = sqlContext.sql(sqlSelectMoretv)

            val mergerDf = df1.unionAll(df2)

            mergerDf.write.parquet(outputPath)
          }else if(!medusaFlag && moretvFlag){
            val moretvDf = sqlContext.read.parquet(logDir2)
            moretvDf.registerTempTable("log_data_2")
            val sqlSelectMoretv = s"select logType, date, datetime, logVersion, event, apkSeries, apkVersion, userId," +
              s"accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime,contentType,videoSid,'' as ip,'' " +
              s"as retrieval,'' as searchText,'' as versionCode,'' as buildDate,'' as pathMain,'' as pathSub, '' as pathSpecial,path, "+
              s"pathParser('detail',path,'path','launcherArea') as launcherAreaFromPath, " +
              s"pathParser('detail',path,'path','launcherAccessLocation') as launcherAccessLocationFromPath, " +
              s"pathParser('detail',path,'path','pageType') as pageTypeFromPath, " +
              s"pathParser('detail',path,'path','pageDetailInfo') as pageDetailInfoFromPath, " +
              s"pathParser('detail',path,'path','accessPath') as accessPathFromPath, " +
              s"pathParser('detail',path,'path','previousSid') as previousSidFromPath, " +
              s"pathParser('detail',path,'path','previousContentType') as previousContentTypeFromPath, " +
              s"pathParser('detail',path,'path','pathProperty') as pathPropertyFromPath, " +
              s"pathParser('detail',path,'path','pathIdentification') as pathIdentificationFromPath, 'moretv' as flag " +
              s" from log_data_2"
            val mergerDf = sqlContext.sql(sqlSelectMoretv)
            mergerDf.write.parquet(outputPath)
          }else if(medusaFlag && !moretvFlag){
            val medusaDf = sqlContext.read.parquet(logDir1)
            medusaDf.registerTempTable("log_data_1")
            val sqlSelectMedusa = s"select logType, date,datetime,logVersion, event, apkSeries, apkVersion, " +
              s"userId,accountId,groupId, promotionChannel, weatherCode, productModel, uploadTime,contentType,videoSid,ip," +
              s"retrieval,searchText,'' as versionCode,buildDate,'' as pathMain,pathSub,pathSpecial,'' as path, " +
              s"pathParser('detail',pathMain,'pathMain','launcherArea') as launcherAreaFromPath, " +
              s"pathParser('detail',pathMain,'pathMain','launcherAccessLocation') as launcherAccessLocationFromPath, " +
              s"pathParser('detail',pathMain,'pathMain','pageType') as pageTypeFromPath, " +
              s"pathParser('detail',pathMain,'pathMain','pageDetailInfo') as pageDetailInfoFromPath, " +
              s"pathParser('detail',pathSub,'pathSub','accessPath') as accessPathFromPath, " +
              s"pathParser('detail',pathSub,'pathSub','previousSid') as previousSidFromPath, " +
              s"pathParser('detail',pathSub,'pathSub','previousContentType') as previousContentTypeFromPath, " +
              s"pathParser('detail',pathSpecial,'pathSpecial','pathProperty') as pathPropertyFromPath, " +
              s"pathParser('detail',pathSpecial,'pathSpecial','pathIdentification') as pathIdentificationFromPath," +
              s" 'medusa' as flag " +
              s"from log_data_1"
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
