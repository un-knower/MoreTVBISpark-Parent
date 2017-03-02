package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar
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
 * This object is used to merge the parquet data of medusa and moretv into one parquet!
 * input: /log/medusa/parquet/$date/detail
 * input: /mbi/parquet/detail/$date
 * output: /log/medusaAndMoretv/parquet/$date/detail
 */
object DetailLogMerger extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p)=>{
        sqlContext.udf.register("pathParser",PathParser.pathParser _)
        sqlContext.udf.register("getSubjectCode",PathParser.getSubjectCodeByPath _)
        sqlContext.udf.register("getSubjectNameBySid",PathParser.getSubjectNameByPath _)
        val startDate = p.startDate
      /*  val logType = "detail"
        val medusaLogType = "detail"
        val moretvLogType = "detail"
        val medusaDir = "/log/medusa/parquet"
        val moretvDir = "/mbi/parquet"*/
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
         /* val logDir1 = s"$medusaDir/$inputDate/$logType"
          val logDir2 = s"$moretvDir/$logType/$inputDate"


          val medusaFlag = FilesInHDFS.fileIsExist(s"$medusaDir/$inputDate",medusaLogType)
          val moretvFlag = FilesInHDFS.fileIsExist(s"$moretvDir/$moretvLogType",inputDate)
          val outputPath = s"/log/medusaAndMoretvMerger/$inputDate/$logType"
*/


          val medusa_input_dir= DataIO.getDataFrameOps.getPath(MEDUSA,LogTypes.DETAIL,inputDate)
          val moretv_input_dir= DataIO.getDataFrameOps.getPath(MORETV,LogTypes.DETAIL,inputDate)
          val outputPath= DataIO.getDataFrameOps.getPath(MERGER,LogTypes.DETAIL,inputDate)
          val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
          val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)


          if(p.deleteOld){
            HdfsUtil.deleteHDFSFile(outputPath)
          }

          if(medusaFlag && moretvFlag){
          /*  val medusaDf = sqlContext.read.parquet(logDir1)
            val moretvDf = sqlContext.read.parquet(logDir2)*/
            val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.DETAIL,inputDate)
            val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.DETAIL,inputDate)

            val medusaColNames = medusaDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
            val moretvColNames = moretvDf.columns.toList.filter(e=>{ParquetSchema.schemaArr.contains(e)}).mkString(",")
            medusaDf.registerTempTable("log_data_1")
            moretvDf.registerTempTable("log_data_2")

            val sqlSelectMedusa = s"select $medusaColNames, " +
              s"pathParser('detail',pathMain,'pathMain','launcherArea') as launcherAreaFromPath, " +
              s"pathParser('detail',pathMain,'pathMain','launcherAccessLocation') as launcherAccessLocationFromPath, " +
              s"pathParser('detail',pathMain,'pathMain','pageType') as pageTypeFromPath, " +
              s"pathParser('detail',pathMain,'pathMain','pageDetailInfo') as pageDetailInfoFromPath, " +
              s"pathParser('detail',pathSub,'pathSub','accessPath') as accessPathFromPath, " +
              s"pathParser('detail',pathSub,'pathSub','previousSid') as previousSidFromPath, " +
              s"pathParser('detail',pathSub,'pathSub','previousContentType') as previousContentTypeFromPath, " +
              s"pathParser('detail',pathSpecial,'pathSpecial','pathProperty') as pathPropertyFromPath, " +
              s"pathParser('detail',pathSpecial,'pathSpecial','pathIdentification') as pathIdentificationFromPath," +
              s"getSubjectCode(pathSpecial,'medusa') as subjectCode," +
              s"getSubjectNameBySid(pathSpecial,'medusa') as subjectCode," +
              s"'medusa' as flag " +
              s"from log_data_1"
            val sqlSelectMoretv = s"select $moretvColNames, "+
              s"pathParser('detail',path,'path','launcherArea') as launcherAreaFromPath, " +
              s"pathParser('detail',path,'path','launcherAccessLocation') as launcherAccessLocationFromPath, " +
              s"pathParser('detail',path,'path','pageType') as pageTypeFromPath, " +
              s"pathParser('detail',path,'path','pageDetailInfo') as pageDetailInfoFromPath, " +
              s"pathParser('detail',path,'path','accessPath') as accessPathFromPath, " +
              s"pathParser('detail',path,'path','previousSid') as previousSidFromPath, " +
              s"pathParser('detail',path,'path','previousContentType') as previousContentTypeFromPath, " +
              s"pathParser('detail',path,'path','pathProperty') as pathPropertyFromPath, " +
              s"pathParser('detail',path,'path','pathIdentification') as pathIdentificationFromPath," +
              s"getSubjectCode(path,'moretv') as subjectCode," +
              s"getSubjectNameBySid(path,'moretv') as subjectName, date as day," +
              s"'moretv' as flag " +
              s" from log_data_2"


            val df1 = sqlContext.sql(sqlSelectMedusa).toJSON
            val df2 = sqlContext.sql(sqlSelectMoretv).toJSON

            val mergerDf = df1.union(df2)

            sqlContext.read.json(mergerDf).write.parquet(outputPath)
          }else if(!medusaFlag && moretvFlag){
            //val moretvDf = sqlContext.read.parquet(logDir2)
            val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MORETV,LogTypes.DETAIL,inputDate)
            val moretvColNames = moretvDf.columns.toList.mkString(",")
            moretvDf.registerTempTable("log_data_2")
            val sqlSelectMoretv = s"select $moretvColNames, "+
              s"pathParser('detail',path,'path','launcherArea') as launcherAreaFromPath, " +
              s"pathParser('detail',path,'path','launcherAccessLocation') as launcherAccessLocationFromPath, " +
              s"pathParser('detail',path,'path','pageType') as pageTypeFromPath, " +
              s"pathParser('detail',path,'path','pageDetailInfo') as pageDetailInfoFromPath, " +
              s"pathParser('detail',path,'path','accessPath') as accessPathFromPath, " +
              s"pathParser('detail',path,'path','previousSid') as previousSidFromPath, " +
              s"pathParser('detail',path,'path','previousContentType') as previousContentTypeFromPath, " +
              s"pathParser('detail',path,'path','pathProperty') as pathPropertyFromPath, " +
              s"pathParser('detail',path,'path','pathIdentification') as pathIdentificationFromPath, 'moretv' as flag " +
              s"getSubjectCode(path,'moretv') as subjectCode," +
              s"getSubjectNameBySid(path,'moretv') as subjectName, date as day,date as day" +
              s" from log_data_2"
            val mergerDf = sqlContext.sql(sqlSelectMoretv)
            mergerDf.write.parquet(outputPath)
          }else if(medusaFlag && !moretvFlag){
            //val medusaDf = sqlContext.read.parquet(logDir1)
            val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.DETAIL,inputDate)

            val medusaColNames = medusaDf.columns.toList.mkString(",")
            medusaDf.registerTempTable("log_data_1")
            val sqlSelectMedusa = s"select $medusaColNames, " +
              s"pathParser('detail',pathMain,'pathMain','launcherArea') as launcherAreaFromPath, " +
              s"pathParser('detail',pathMain,'pathMain','launcherAccessLocation') as launcherAccessLocationFromPath, " +
              s"pathParser('detail',pathMain,'pathMain','pageType') as pageTypeFromPath, " +
              s"pathParser('detail',pathMain,'pathMain','pageDetailInfo') as pageDetailInfoFromPath, " +
              s"pathParser('detail',pathSub,'pathSub','accessPath') as accessPathFromPath, " +
              s"pathParser('detail',pathSub,'pathSub','previousSid') as previousSidFromPath, " +
              s"pathParser('detail',pathSub,'pathSub','previousContentType') as previousContentTypeFromPath, " +
              s"pathParser('detail',pathSpecial,'pathSpecial','pathProperty') as pathPropertyFromPath, " +
              s"pathParser('detail',pathSpecial,'pathSpecial','pathIdentification') as pathIdentificationFromPath," +
              s"getSubjectCode(pathSpecial,'medusa') as subjectCode," +
              s"getSubjectNameBySid(pathSpecial,'medusa') as subjectName," +
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
