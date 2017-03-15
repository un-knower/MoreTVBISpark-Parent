package com.moretv.bi.report.medusa.medusaAndMoretvParquetMerger

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.LogTypes
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.PathParser
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by baozhi.wang on 2017/3/14.
  * This object is used to merge the parquet data of medusa and moretv into one parquet!
  * input: /log/medusa/parquet/$date/play
  * input: /mbi/parquet/playview/$date
  * output: /log/medusaAndMoretv/parquet/$date/playview2filterETL
  *
  * play事实表的作用：
  * 1.解析出所有分析脚本可以使用的subject code
  *
  */
object PlayViewLogMergerETL extends BaseClass {
  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        sqlContext.udf.register("pathParser", PathParser.pathParser _)
        sqlContext.udf.register("getSubjectCode", PathParser.getSubjectCodeByPathETL _)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i => {
          val inputDate = DateFormatUtils.readFormat.format(cal.getTime)
          val medusa_input_dir = DataIO.getDataFrameOps.getPath(MEDUSA, LogTypes.PLAY, inputDate)
          val moretv_input_dir = DataIO.getDataFrameOps.getPath(MORETV, LogTypes.PLAYVIEW, inputDate)
          val outputPath = DataIO.getDataFrameOps.getPath(MERGER, LogTypes.PLAY_VIEW_2_FILTER_ETL, inputDate)
          val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
          val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)

          if (p.deleteOld) {
            HdfsUtil.deleteHDFSFile(outputPath)
          }
          if (medusaFlag && moretvFlag) {
            val medusaDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.PLAY, inputDate)
            val moretvDf = DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MORETV, LogTypes.PLAYVIEW, inputDate)
            val medusaColNames = medusaDf.columns.toList.filter(e => {
              ParquetSchema.schemaArr.contains(e)
            }).mkString(",")
            val moretvColNames = moretvDf.columns.toList.filter(e => {
              ParquetSchema.schemaArr.contains(e)
            }).mkString(",")
            medusaDf.registerTempTable("medusa_table")
            moretvDf.registerTempTable("moretv_table")

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
              s"'medusa' as flag " +
              " from medusa_table"
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
              s"date as day," +
              s"'moretv' as flag from moretv_table"

            val df1 = sqlContext.sql(sqlSelectMedusa).toJSON
            val df2 = sqlContext.sql(sqlSelectMoretv).toJSON

            val mergerDf = df1.union(df2)
            sqlContext.read.json(mergerDf).write.parquet(outputPath)
          } else if (!medusaFlag && moretvFlag) {
            throw new RuntimeException("medusaFlag is false")
          } else if (medusaFlag && !moretvFlag) {
            throw new RuntimeException("moretvFlag is false")
          }
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
