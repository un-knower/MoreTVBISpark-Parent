package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar

import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.{PathParser, PathParserDimension, UDFConstantDimension}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by wangbaozhi on 2016/12/12,
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * and get kinds of dimension from pathMain to generate new columns.
  * input: /log/medusa/parquet/$date/play
  * input: /mbi/parquet/playview/$date
  * output: /log/medusaAndMoretvMergerDimension/$date/playview2filter
  */
object PlayViewLogDimensionUnionAll extends BaseClass{
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(this,args)
  }

   override def execute(args: Array[String]) {
     println("-------------------------michael test--------------")
     ParamsParseUtil.parse(args) match {
       case Some(p)=>{
         sqlContext.udf.register("pathParser",PathParser.pathParser _)
         sqlContext.udf.register("pathParserDimension",PathParserDimension.pathParserDimension _)
         sqlContext.udf.register("getSubjectCode",PathParser.getSubjectCodeByPath _)
         sqlContext.udf.register("getSubjectNameBySid",PathParser.getSubjectNameByPath _)
         val startDate = p.startDate
         val medusaLogType = "play"
         val moretvLogType = "playview"
        /* val medusaDir ="/log/medusa/parquet"
         val moretvDir = "/mbi/parquet"*/
        val medusaDir = "/data_warehouse/medusa_test/parquet"
         val moretvDir = "/data_warehouse/helios_test/parquet"
         val outputDir =  UDFConstantDimension.MEDUSA_BIG_FACT_TABLE_DIR
         val outLogType = UDFConstantDimension.MEDUSA_BIG_FACT_TABLE_PLAY_TYPE

         val cal = Calendar.getInstance()
         cal.setTime(DateFormatUtils.readFormat.parse(startDate))

         (0 until p.numOfDays).foreach(i=>{
           val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
           //val logDir1 = s"/log/medusa/parquet/$inputDate/$medusaLogType"
           val logDir1 = s"$medusaDir/$inputDate/$medusaLogType"

           //val logDir2 = s"/mbi/parquet/$moretvLogType/$inputDate"
           val logDir2 = s"$moretvDir/$inputDate/$moretvLogType"
           println("logDir1:"+logDir1)
           println("logDir2:"+logDir2)


           val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",medusaLogType)
           //val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$moretvLogType",inputDate)
           val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$inputDate",moretvLogType)
           val outputPath = s"$outputDir/$inputDate/$outLogType"

           if(p.deleteOld){
             HdfsUtil.deleteHDFSFile(outputPath)
           }

           if(medusaFlag && moretvFlag){
             println("-------------------------michael 1--------------")
             val medusaDf = sqlContext.read.parquet(logDir1)
             val moretvDf = sqlContext.read.parquet(logDir2)
             val medusaColNames = medusaDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
             val moretvColNames = moretvDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")

             println("medusaColNames:"+medusaColNames)

             println("moretvColNames:"+moretvColNames)

             //------------- for use dataframe union all
             val medusaColumnList=medusaDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)})
             val medusaNotExistColumn= ParquetSchema.schemaArr.filter(e=>{!medusaColumnList.contains(e)})
             val moretvColumnList=moretvDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)})
             val moretvNotExistColumn= ParquetSchema.schemaArr.filter(e=>{!moretvColumnList.contains(e)})

               println(medusaColumnList.size)
               println(medusaNotExistColumn.size)
               println(moretvColumnList.size)
               println(moretvNotExistColumn.size)



             var medusaNotExistColumnStr:String=""
             for(e<-medusaNotExistColumn){
               medusaNotExistColumnStr+=s"'' as $e,"
             }
             println("medusaNotExistColumnStr:"+medusaNotExistColumnStr)

             var moreNotExistColumnStr:String=""
             for(e<-moretvNotExistColumn){
               moreNotExistColumnStr+=s"'' as $e,"
             }
             println("moreNotExistColumnStr:"+moreNotExistColumnStr)
             //-------------

             medusaDf.registerTempTable("log_data_1")
             moretvDf.registerTempTable("log_data_2")
             val sqlSelectMedusa = s"select $medusaNotExistColumnStr $medusaColNames, " +
               s"pathParser('play',pathMain,'pathMain','launcherArea') as launcherAreaFromPath, " +
               s"pathParser('play',pathMain,'pathMain','launcherAccessLocation') as launcherAccessLocationFromPath, " +
               s"pathParser('play',pathMain,'pathMain','pageType') as pageTypeFromPath, " +
               s"pathParser('play',pathMain,'pathMain','pageDetailInfo') as pageDetailInfoFromPath, " +
               s"pathParser('play',pathSub,'pathSub','accessPath') as accessPathFromPath, " +
               s"pathParser('play',pathSub,'pathSub','previousSid') as previousSidFromPath, " +
               s"pathParser('play',pathSub,'pathSub','previousContentType') as previousContentTypeFromPath, " +
               s"pathParser('play',pathSpecial,'pathSpecial','pathProperty') as pathPropertyFromPath, " +
               s"pathParser('play',pathSpecial,'pathSpecial','pathIdentification') as pathIdentificationFromPath," +
               s"getSubjectCode(pathSpecial,'medusa') as subjectCode," +
               s"getSubjectNameBySid(pathSpecial,'medusa') as subjectName," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.SEARCH_KEYWORD+"') as " +UDFConstantDimension.SEARCH_KEYWORD+"," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.SEARCH_FROM+"') as " +UDFConstantDimension.SEARCH_FROM+"," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.MAIN_CATEGORY+"') as " +UDFConstantDimension.MAIN_CATEGORY+"," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.SUB_CATEGORY+"') as " +UDFConstantDimension.SUB_CATEGORY+"," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.FILTER_CATEGORY_1+"') as "+UDFConstantDimension.FILTER_CATEGORY_1+"," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.FILTER_CATEGORY_2+"') as "+UDFConstantDimension.FILTER_CATEGORY_2+","+
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.FILTER_CATEGORY_3+"') as "+UDFConstantDimension.FILTER_CATEGORY_3+"," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.FILTER_CATEGORY_4+"') as "+UDFConstantDimension.FILTER_CATEGORY_4+"," +
               s"pathParserDimension('play',pathSub,'pathSub','accessPath') as "+UDFConstantDimension.RECOMMEND_SOURCE_TYPE+"," +
               s"pathParserDimension('play',pathSub,'pathSub','previousSid') as "+UDFConstantDimension.RECOMMEND_PROPERTY+"," +
               s"pathParserDimension('play',pathSub,'pathSub','previousContentType') as "+UDFConstantDimension.RECOMMEND_PRE_CONTENT_TYPE+"," +
               s"pathParserDimension('play',pathSpecial,'pathSpecial','pathProperty') as "+UDFConstantDimension.SPECIAL_SOURCE_TYPE+"," +
               s"getSubjectCode(pathSpecial,'medusa') as  "+UDFConstantDimension.SPECIAL_SOURCE_ID+"," +
               s"getSubjectNameBySid(pathSpecial,'medusa') as "+UDFConstantDimension.SPECIAL_SOURCE_NAME+"," +
               s"pathParserDimension('play',pathMain,'pathMain','launcherArea') as "+UDFConstantDimension.SOURCE_LAUNCHER_AREA+"," +
               s"pathParserDimension('play',pathMain,'pathMain','launcherAccessLocation') as "+UDFConstantDimension.SOURCE_LAUNCHER_POSITION+"," +
               s"pathParserDimension('play',pathMain,'pathMain','"+UDFConstantDimension.SOURCE_LAUNCHER_POSITION_INDEX+"') as "+UDFConstantDimension.SOURCE_LAUNCHER_POSITION_INDEX+"," +
               s" 'medusa' as flag " +
               s" from log_data_1"
             println(sqlSelectMedusa)
             val sqlSelectMoretv = s"select $moreNotExistColumnStr $moretvColNames," +
               s"pathParser('playview',path,'path','launcherArea') as launcherAreaFromPath, " +
               s"pathParser('playview',path,'path','launcherAccessLocation') as launcherAccessLocationFromPath, " +
               s"pathParser('playview',path,'path','pageType') as pageTypeFromPath, " +
               s"pathParser('playview',path,'path','pageDetailInfo') as pageDetailInfoFromPath, " +
               s"pathParser('playview',path,'path','accessPath') as accessPathFromPath, " +
               s"pathParser('playview',path,'path','previousSid') as previousSidFromPath, " +
               s"pathParser('playview',path,'path','previousContentType') as previousContentTypeFromPath, " +
               s"pathParser('playview',path,'path','pathProperty') as pathPropertyFromPath, " +
               s"pathParser('playview',path,'path','pathIdentification') as pathIdentificationFromPath," +
               s"getSubjectCode(path,'moretv') as subjectCode," +
               s"getSubjectNameBySid(path,'moretv') as subjectName," +
               s"date as day,"+
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.SEARCH_KEYWORD+"') as " +UDFConstantDimension.SEARCH_KEYWORD+"," +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.SEARCH_FROM+"') as " +UDFConstantDimension.SEARCH_FROM+"," +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.MAIN_CATEGORY+"') as " +UDFConstantDimension.MAIN_CATEGORY+"," +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.SUB_CATEGORY+"') as " +UDFConstantDimension.SUB_CATEGORY+"," +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.FILTER_CATEGORY_1+"') as "+UDFConstantDimension.FILTER_CATEGORY_1+"," +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.FILTER_CATEGORY_2+"') as "+UDFConstantDimension.FILTER_CATEGORY_2+","+
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.FILTER_CATEGORY_3+"') as "+UDFConstantDimension.FILTER_CATEGORY_3+"," +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.FILTER_CATEGORY_4+"') as "+UDFConstantDimension.FILTER_CATEGORY_4+"," +
               s"pathParserDimension('playview',path,'path', 'accessPath') as "+UDFConstantDimension.RECOMMEND_SOURCE_TYPE+"," +
               s"pathParserDimension('playview',path,'path', 'previousSid') as "+UDFConstantDimension.RECOMMEND_PROPERTY+"," +
               s"pathParserDimension('playview',path,'path', 'previousContentType') as "+UDFConstantDimension.RECOMMEND_PRE_CONTENT_TYPE+"," +
               s"pathParser('playview',path,'path','pathProperty') as "+UDFConstantDimension.SPECIAL_SOURCE_TYPE+"," +
               s"getSubjectCode(path,'moretv') as  "+UDFConstantDimension.SPECIAL_SOURCE_ID+"," +
               s"getSubjectNameBySid(path,'moretv') as "+UDFConstantDimension.SPECIAL_SOURCE_NAME+"," +
               s"pathParserDimension('playview',path,'path','launcherArea') as "+UDFConstantDimension.SOURCE_LAUNCHER_AREA+"," +
               s"pathParserDimension('playview',path,'path','launcherAccessLocation') as "+UDFConstantDimension.SOURCE_LAUNCHER_POSITION+"," +
               s"pathParserDimension('playview',path,'path','"+UDFConstantDimension.SOURCE_LAUNCHER_POSITION_INDEX+"') as "+UDFConstantDimension.SOURCE_LAUNCHER_POSITION_INDEX+"," +
               s" 'moretv' as flag "+
               s" from log_data_2"
             println(sqlSelectMoretv)
             /*val df1 = sqlContext.sql(sqlSelectMedusa).repartition(20).toJSON
             val df2 = sqlContext.sql(sqlSelectMoretv).repartition(20).toJSON
             val mergerDf = df1.union(df2)
             sqlContext.read.json(mergerDf).repartition(20).write.parquet(outputPath)*/
              val df1 = sqlContext.sql(sqlSelectMedusa)
              val df2 = sqlContext.sql(sqlSelectMoretv)
              df1.unionAll(df2).write.parquet(outputPath)

           }else if(!medusaFlag && moretvFlag){
             println("-------------------------michael 2--------------")
             val moretvDf = sqlContext.read.parquet(logDir2)
             val moretvColNames = moretvDf.columns.toList.mkString(",")
             moretvDf.registerTempTable("log_data_2")
             val sqlSelectMoretv = s"select $moretvColNames," +
               s"pathParser('playview',path,'path','launcherArea') as launcherAreaFromPath, " +
               s"pathParser('playview',path,'path','launcherAccessLocation') as launcherAccessLocationFromPath, " +
               s"pathParser('playview',path,'path','pageType') as pageTypeFromPath, " +
               s"pathParser('playview',path,'path','pageDetailInfo') as pageDetailInfoFromPath, " +
               s"pathParser('playview',path,'path','accessPath') as accessPathFromPath, " +
               s"pathParser('playview',path,'path','previousSid') as previousSidFromPath, " +
               s"pathParser('playview',path,'path','previousContentType') as previousContentTypeFromPath, " +
               s"pathParser('playview',path,'path','pathProperty') as pathPropertyFromPath, " +
               s"pathParser('playview',path,'path','pathIdentification') as pathIdentificationFromPath," +
               s"getSubjectCode(path,'moretv') as subjectCode," +
               s"getSubjectNameBySid(path,'moretv') as subjectName,date as day" +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.SEARCH_KEYWORD+"') as " +UDFConstantDimension.SEARCH_KEYWORD+"," +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.SEARCH_FROM+"') as " +UDFConstantDimension.SEARCH_FROM+"," +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.MAIN_CATEGORY+"') as " +UDFConstantDimension.MAIN_CATEGORY+"," +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.SUB_CATEGORY+"') as " +UDFConstantDimension.SUB_CATEGORY+"," +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.FILTER_CATEGORY_1+"') as "+UDFConstantDimension.FILTER_CATEGORY_1+"," +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.FILTER_CATEGORY_2+"') as "+UDFConstantDimension.FILTER_CATEGORY_2+","+
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.FILTER_CATEGORY_3+"') as "+UDFConstantDimension.FILTER_CATEGORY_3+"," +
               s"pathParserDimension('playview',path,'path', '"+UDFConstantDimension.FILTER_CATEGORY_4+"') as "+UDFConstantDimension.FILTER_CATEGORY_4+"," +
               s"pathParserDimension('playview',path,'path', 'accessPath') as "+UDFConstantDimension.RECOMMEND_SOURCE_TYPE+"," +
               s"pathParserDimension('playview',path,'path', 'previousSid') as "+UDFConstantDimension.RECOMMEND_PROPERTY+"," +
               s"pathParserDimension('playview',path,'path', 'previousContentType') as "+UDFConstantDimension.RECOMMEND_PRE_CONTENT_TYPE+"," +
               s"pathParser('playview',path,'path','pathProperty') as "+UDFConstantDimension.SPECIAL_SOURCE_TYPE+"," +
               s"getSubjectCode(path,'moretv') as  "+UDFConstantDimension.SPECIAL_SOURCE_ID+"," +
               s"getSubjectNameBySid(path,'moretv') as "+UDFConstantDimension.SPECIAL_SOURCE_NAME+"," +
               s"pathParserDimension('playview',path,'path','launcherArea') as "+UDFConstantDimension.SOURCE_LAUNCHER_AREA+"," +
               s"pathParserDimension('playview',path,'path','launcherAccessLocation') as "+UDFConstantDimension.SOURCE_LAUNCHER_POSITION+"," +
               s"pathParserDimension('playview',path,'path','"+UDFConstantDimension.SOURCE_LAUNCHER_POSITION_INDEX+"') as "+UDFConstantDimension.SOURCE_LAUNCHER_POSITION_INDEX+"," +
               s"'moretv' as flag from log_data_2"

             val mergerDf=sqlContext.sql(sqlSelectMoretv)
             mergerDf.write.parquet(outputPath)

           }else if(medusaFlag && !moretvFlag){
             println("-------------------------michael 3--------------")
             val medusaDf = sqlContext.read.parquet(logDir1)
             val medusaColNames = medusaDf.columns.toList.mkString(",")
             medusaDf.registerTempTable("log_data_1")
             val sqlSelectMedusa = s"select $medusaColNames, " +
               s"pathParser('play',pathMain,'pathMain','launcherArea') as launcherAreaFromPath, " +
               s"pathParser('play',pathMain,'pathMain','launcherAccessLocation') as launcherAccessLocationFromPath, " +
               s"pathParser('play',pathMain,'pathMain','pageType') as pageTypeFromPath, " +
               s"pathParser('play',pathMain,'pathMain','pageDetailInfo') as pageDetailInfoFromPath, " +
               s"pathParser('play',pathSub,'pathSub','accessPath') as accessPathFromPath, " +
               s"pathParser('play',pathSub,'pathSub','previousSid') as previousSidFromPath, " +
               s"pathParser('play',pathSub,'pathSub','previousContentType') as previousContentTypeFromPath, " +
               s"pathParser('play',pathSpecial,'pathSpecial','pathProperty') as pathPropertyFromPath, " +
               s"pathParser('play',pathSpecial,'pathSpecial','pathIdentification') as pathIdentificationFromPath," +
               s"getSubjectCode(pathSpecial,'medusa') as subjectCode," +
               s"getSubjectNameBySid(pathSpecial,'medusa') as subjectName," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.SEARCH_KEYWORD+"') as " +UDFConstantDimension.SEARCH_KEYWORD+"," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.SEARCH_FROM+"') as " +UDFConstantDimension.SEARCH_FROM+"," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.MAIN_CATEGORY+"') as " +UDFConstantDimension.MAIN_CATEGORY+"," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.SUB_CATEGORY+"') as " +UDFConstantDimension.SUB_CATEGORY+"," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.FILTER_CATEGORY_1+"') as "+UDFConstantDimension.FILTER_CATEGORY_1+"," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.FILTER_CATEGORY_2+"') as "+UDFConstantDimension.FILTER_CATEGORY_2+","+
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.FILTER_CATEGORY_3+"') as "+UDFConstantDimension.FILTER_CATEGORY_3+"," +
               s"pathParserDimension('play',pathMain,'pathMain', '"+UDFConstantDimension.FILTER_CATEGORY_4+"') as "+UDFConstantDimension.FILTER_CATEGORY_4+"," +
               s"pathParserDimension('play',pathSub,'pathSub','accessPath') as "+UDFConstantDimension.RECOMMEND_SOURCE_TYPE+"," +
               s"pathParserDimension('play',pathSub,'pathSub','previousSid') as "+UDFConstantDimension.RECOMMEND_PROPERTY+"," +
               s"pathParserDimension('play',pathSub,'pathSub','previousContentType') as "+UDFConstantDimension.RECOMMEND_PRE_CONTENT_TYPE+"," +
               s"pathParserDimension('play',pathSpecial,'pathSpecial','pathProperty') as "+UDFConstantDimension.SPECIAL_SOURCE_TYPE+"," +
               s"getSubjectCode(pathSpecial,'medusa') as  "+UDFConstantDimension.SPECIAL_SOURCE_ID+"," +
               s"getSubjectNameBySid(pathSpecial,'medusa') as "+UDFConstantDimension.SPECIAL_SOURCE_NAME+"," +
               s"pathParserDimension('play',pathMain,'pathMain','launcherArea') as "+UDFConstantDimension.SOURCE_LAUNCHER_AREA+"," +
               s"pathParserDimension('play',pathMain,'pathMain','launcherAccessLocation') as "+UDFConstantDimension.SOURCE_LAUNCHER_POSITION+"," +
               s"pathParserDimension('play',pathMain,'pathMain','"+UDFConstantDimension.SOURCE_LAUNCHER_POSITION_INDEX+"') as "+UDFConstantDimension.SOURCE_LAUNCHER_POSITION_INDEX+"," +
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
